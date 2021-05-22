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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.tablestore.adapter.RecordSink;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.tablestore.TablestoreValueUtil.convert;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.System.currentTimeMillis;

public class TablestoreRecordSink
        implements RecordSink
{
    private static final Logger log = Logger.get(TablestoreRecordSink.class);
    public static final int BATCH_WRITE_COUNT_THRESHOLD = 200;

    private final ConnectorSession session;
    private final String queryId;
    private final String ramId;
    private final SchemaTableName schemaTableName;
    private final List<TablestoreColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final TablestoreFacade tablestoreFacade;
    private final long startTimestamp;
    private final boolean insertUseUpdate;

    private int fieldIndex;
    private int totalRowCount;
    private int batchRowCount;
    private long apiNano;
    private RowChange currentRow;
    private PrimaryKeyBuilder pkBuilder;
    private BatchWriteRowRequest batchWrite;

    public TablestoreRecordSink(TablestoreInsertTableHandle handle, TablestoreFacade tablestoreFacade, ConnectorSession session)
    {
        this.schemaTableName = handle.getSchemaTableName();
        this.columnHandles = handle.getColumnHandleList();
        this.tablestoreFacade = tablestoreFacade;
        this.insertUseUpdate = TablestoreFacade.enableInsertAsUpdate(session);
        this.session = session;
        this.queryId = session.getQueryId();
        this.ramId = session.getUser();
        this.columnTypes = columnHandles.stream().map(TablestoreColumnHandle::getColumnType).collect(Collectors.toList());
        this.startTimestamp = currentTimeMillis();

        batchWrite = new BatchWriteRowRequest();
        fieldIndex = -1;
        batchRowCount = 0;
        totalRowCount = 0;
        apiNano = 0;

        log.info("Created a new tablestore record sink, queryId=%s ramUserId=%s table=%s", session.getQueryId(), session.getUser(), schemaTableName);
    }

    private RowChange createNewRowChange()
    {
        return insertUseUpdate ? new RowUpdateChange(schemaTableName.getTableName()) : new RowPutChange(schemaTableName.getTableName());
    }

    @Override
    public void beginRecord()
    {
        checkState(fieldIndex == -1, "already in record");
        fieldIndex = 0;

        currentRow = createNewRowChange();

        pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
    }

    @Override
    public void finishRecord()
    {
        checkState(fieldIndex == columnHandles.size(),
                "not all fields set, fieldIndex=%s, fieldCount=%s", fieldIndex, columnHandles.size());

        currentRow.setPrimaryKey(pkBuilder.build());
        batchWrite.addRowChange(currentRow);
        batchRowCount++;
        totalRowCount++;

        if (batchRowCount == BATCH_WRITE_COUNT_THRESHOLD) {
            submitBatch();
        }
        fieldIndex = -1;
    }

    private void submitBatch()
    {
        long nano = System.nanoTime();
        tablestoreFacade.updateBatchRows(session, schemaTableName, batchWrite);
        long delta = System.nanoTime() - nano;
        apiNano += delta;
        batchWrite = new BatchWriteRowRequest();
        batchRowCount = 0;
    }

    @Override
    public void appendNull()
    {
        appendAny(null);
    }

    private void appendAny(Object value)
    {
        checkState(fieldIndex < columnHandles.size(), "There are only " + columnHandles.size() + " fields to append, but we appended too many values");
        TablestoreColumnHandle c = columnHandles.get(fieldIndex);
        if (c.isPrimaryKey()) {
            if (value == null) {
                throw new UnsupportedOperationException("Primary key[" + c + "] should not be appended null, with respect to TableStore");
            }
            PrimaryKeyValue pkv = convert(c.getColumnType(), true, value);
            pkBuilder.addPrimaryKeyColumn(c.getColumnName(), pkv);
        }
        else {
            if (value != null) {
                ColumnValue cv = convert(c.getColumnType(), false, value);
                if (insertUseUpdate) {
                    ((RowUpdateChange) currentRow).put(c.getColumnName(), cv);
                }
                else {
                    ((RowPutChange) currentRow).addColumn(c.getColumnName(), cv);
                }
            }
        }
        fieldIndex++;
    }

    @Override
    public void appendBoolean(boolean value)
    {
        appendAny(value);
    }

    @Override
    public void appendLong(long value)
    {
        appendAny(value);
    }

    @Override
    public void appendDouble(double value)
    {
        appendAny(value);
    }

    @Override
    public void appendString(byte[] value)
    {
        appendAny(value);
    }

    @Override
    public void appendObject(Object value)
    {
        appendAny(value);
    }

    @Override
    public Collection<Slice> commit()
    {
        if (batchRowCount > 0) {
            submitBatch();
        }
        log.info("Total insert %s records, queryId=%s ramUserId=%s table=%s totalCost=%sms apiCost=%sms",
                totalRowCount, queryId, ramId, schemaTableName, currentTimeMillis() - startTimestamp, apiNano / 1000_000);
        return ImmutableList.of();
    }

    @Override
    public void rollback()
    {
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }
}
