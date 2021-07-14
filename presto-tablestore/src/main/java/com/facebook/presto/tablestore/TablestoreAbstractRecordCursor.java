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

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;

import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.tablestore.TablestoreFacade.NANO_SECS_IN_ONE_MS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;

public abstract class TablestoreAbstractRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(TablestoreAbstractRecordCursor.class);
    private static final long NANOS_PER_MILLI_SECOND = 1000_000;

    protected interface Callable<T>
    {
        T call();
    }

    protected final TablestoreColumnHandle[] columnHandles;
    protected final List<TablestoreColumnHandle> primaryKeys;
    protected final long startTimestamp;
    protected final String tableName;
    protected final ConnectorSession session;
    protected final TablestoreFacade tablestoreFacade;
    protected final TablestoreSplit split;
    protected final TablestoreTableHandle tableHandle;
    protected final boolean enableLooseCast;

    protected boolean closed;
    protected int rows;
    protected long apiInNanos;
    protected long readNano;

    public TablestoreAbstractRecordCursor(TablestoreFacade tablestoreFacade, ConnectorSession session, TablestoreSplit split, List<TablestoreColumnHandle> columnHandles)
    {
        this.tablestoreFacade = tablestoreFacade;
        this.session = requireNonNull(session, "session is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null")
                .toArray(new TablestoreColumnHandle[columnHandles.size()]);
        this.split = split;
        this.tableHandle = split.getTableHandle();
        this.tableName = requireNonNull(tableHandle.getTableName(), "tableName is null");
        this.primaryKeys = columnHandles.stream()
                .filter(TablestoreColumnHandle::isPrimaryKey)
                .sorted(Comparator.comparingInt(TablestoreColumnHandle::getPkPosition))
                .collect(Collectors.toList());
        this.enableLooseCast = TablestoreFacade.enableLooseCast(session);
        this.startTimestamp = currentTimeMillis();
    }

    protected <T> T callAndRecord(int field, Callable<T> call)
    {
        long t1 = nanoTime();
        try {
            checkStates(field);
            return call.call();
        }
        catch (Throwable t) {
            throw new RuntimeException("row[pk=" + currentPrimaryKeyInfo() + "] field[index=" + field + "] -> " + t.getMessage(), t);
        }
        finally {
            readNano += nanoTime() - t1;
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        checkClosed();
        return (currentTimeMillis() - startTimestamp) * NANOS_PER_MILLI_SECOND;
    }

    protected long getTotalCostMillis()
    {
        return getReadTimeNanos() / NANOS_PER_MILLI_SECOND;
    }

    protected long getApiCostMillis()
    {
        return apiInNanos / NANOS_PER_MILLI_SECOND;
    }

    protected long getReadCostMillis()
    {
        return readNano / NANOS_PER_MILLI_SECOND;
    }

    @Override
    public Type getType(int field)
    {
        checkStates(field);
        return columnHandles[field].getColumnType();
    }

    protected String getColumnName(int index)
    {
        return columnHandles[index].getColumnName();
    }

    protected boolean isPrimaryKey(int index)
    {
        return columnHandles[index].isPrimaryKey();
    }

    protected abstract boolean hasNext();

    protected abstract void next();

    protected abstract String currentPrimaryKeyInfo();

    protected String toString(Object value)
    {
        if (value == null) {
            return "null";
        }
        else if (value instanceof byte[]) {
            String encodeValue = Base64.getEncoder().encodeToString((byte[]) value);
            return encodeValue.length() > 24 ? encodeValue.substring(0, 24) + "..." : encodeValue;
        }
        else {
            return String.valueOf(value);
        }
    }

    @Override
    public boolean advanceNextPosition()
    {
        checkClosed();
        String queryId = session.getQueryId();
        String fmt = "queryId=%s scannedRows=%s elapsedMills=%d msg=%s";

        try {
            long t1 = nanoTime();
            if (hasNext()) {
                next();
                long delta = System.nanoTime() - t1;

                if (delta >= 10 * NANO_SECS_IN_ONE_MS) {
                    log.debug("split scan new fetch, queryId=%s currentRows=%s deltaApiCost=%sms", queryId, rows, delta / 1000_1000.0);
                }
                apiInNanos += delta;
                rows++;
                return true;
            }
            else {
                log.info("split scan over, queryId={} scannedRows=%s totalCost=%sms apiCost=%sms readCost=%sms completedBytes=%s table=%s",
                        queryId, rows, getTotalCostMillis(), getApiCostMillis(), getReadCostMillis(), getCompletedBytes(), tableName);
                return false;
            }
        }
        catch (TableStoreException e) {
            String err = "Error reading TableStore, traceId=%s requestId=%s " + fmt;
            err = String.format(err, e.getTraceId(), e.getRequestId(), queryId, rows, getReadCostMillis(), e.getMessage());
            log.error(err, e);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, err, e);
        }
        catch (ClientException e) {
            String err = "Error reading TableStore, traceId=%s " + fmt;
            err = String.format(err, e.getTraceId(), queryId, rows, getReadCostMillis(), e.getMessage());
            log.error(err, e);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, err, e);
        }
    }

    protected void checkClosed()
    {
        checkState(!closed, "cursor is closed.");
    }

    protected void checkStates(int field)
    {
        checkClosed();
        checkArgument(field >= 0 && field < columnHandles.length, "Invalid field index: %s", field);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
    }
}
