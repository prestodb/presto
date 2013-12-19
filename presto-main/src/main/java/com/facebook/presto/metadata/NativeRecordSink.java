/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.Type.fromColumnType;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NativeRecordSink
        implements RecordSink
{
    private final String nodeId;
    private final ColumnFileHandle fileHandle;
    private final LocalStorageManager storageManager;
    private final PageBuilder pageBuilder;

    private int field = -1;

    public NativeRecordSink(String nodeId, ColumnFileHandle fileHandle, LocalStorageManager storageManager, List<ColumnType> columnTypes)
    {
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
        this.fileHandle = checkNotNull(fileHandle, "fileHandle is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        pageBuilder = new PageBuilder(toTupleInfos(columnTypes));
    }

    @Override
    public void beginRecord()
    {
        checkState(field == -1, "already in record");
        field = 0;
    }

    @Override
    public void finishRecord()
    {
        checkState(field != -1, "not in record");
        checkState(field == fileHandle.getFieldCount(), "not all fields set");
        field = -1;

        if (pageBuilder.isFull()) {
            fileHandle.append(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    @Override
    public void setNextNull()
    {
        nextColumn().appendNull();
    }

    @Override
    public void setNextBoolean(boolean value)
    {
        nextColumn().append(value);
    }

    @Override
    public void setNextLong(long value)
    {
        nextColumn().append(value);
    }

    @Override
    public void setNextDouble(double value)
    {
        nextColumn().append(value);
    }

    @Override
    public void setNextString(byte[] value)
    {
        nextColumn().append(value);
    }

    @Override
    public String commit()
    {
        checkState(field == -1, "record not finished");

        if (!pageBuilder.isEmpty()) {
            fileHandle.append(pageBuilder.build());
        }

        try {
            storageManager.commit(fileHandle);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return Joiner.on(':').join(nodeId, fileHandle.getShardUuid());
    }

    private BlockBuilder nextColumn()
    {
        checkState(field != -1, "not in record");
        checkState(field < fileHandle.getFieldCount(), "all fields already set");
        BlockBuilder builder = pageBuilder.getBlockBuilder(field);
        field++;
        return builder;
    }

    private static List<TupleInfo> toTupleInfos(List<ColumnType> columnTypes)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ColumnType columnType : columnTypes) {
            tupleInfos.add(new TupleInfo(fromColumnType(columnType)));
        }
        return tupleInfos.build();
    }
}
