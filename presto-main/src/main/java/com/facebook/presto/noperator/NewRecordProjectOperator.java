/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.BYTE;

public class NewRecordProjectOperator
        implements NewOperator
{
    private static final int ROWS_PER_REQUEST = 16384;
    private final OperatorContext operatorContext;
    private final RecordCursor cursor;
    private final List<TupleInfo> tupleInfos;
    private final PageBuilder pageBuilder;
    private boolean finishing;
    private long completedBytes;

    public NewRecordProjectOperator(OperatorContext operatorContext, RecordSet recordSet)
    {
        this(operatorContext, recordSet.getColumnTypes(), recordSet.cursor());
    }

    public NewRecordProjectOperator(OperatorContext operatorContext, List<ColumnType> columnTypes, RecordCursor cursor)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.cursor = checkNotNull(cursor, "cursor is null");

        // project each field into a separate channel
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ColumnType columnType : columnTypes) {
            tupleInfos.add(new TupleInfo(Type.fromColumnType(columnType)));
        }
        this.tupleInfos = tupleInfos.build();

        pageBuilder = new PageBuilder(getTupleInfos());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    public RecordCursor getCursor()
    {
        return cursor;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public void finish()
    {
        finishing = true;
        cursor.close();
    }

    @Override
    public boolean isFinished()
    {
        return finishing && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        if (!finishing) {
            int i = 0;
            for (; i < ROWS_PER_REQUEST; i++) {
                if (pageBuilder.isFull()) {
                    break;
                }

                if (!cursor.advanceNextPosition()) {
                    finishing = true;
                    break;
                }

                for (int column = 0; column < tupleInfos.size(); column++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(column);
                    if (cursor.isNull(column)) {
                        output.appendNull();
                    }
                    else {
                        Type type = getTupleInfos().get(column).getTypes().get(0);
                        switch (type) {
                            case BOOLEAN:
                                output.append(cursor.getBoolean(column));
                                break;
                            case FIXED_INT_64:
                                output.append(cursor.getLong(column));
                                break;
                            case DOUBLE:
                                output.append(cursor.getDouble(column));
                                break;
                            case VARIABLE_BINARY:
                                output.append(cursor.getString(column));
                                break;
                            default:
                                   throw new AssertionError("unimplemented type: " + type);
                        }
                    }
                }
            }

            long bytesProcessed = cursor.getCompletedBytes() - completedBytes;
            operatorContext.recordGeneratedInput(new DataSize(bytesProcessed, BYTE), i);
            completedBytes += bytesProcessed;
        }

        // only return a full page is buffer is full or we are finishing
        if (pageBuilder.isEmpty() || (!finishing && !pageBuilder.isFull())) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }
}
