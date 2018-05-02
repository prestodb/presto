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
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LookAheadResultIterator;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.RoundRobinResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.plugin.phoenix.PhoenixClient.getQueryPlan;
import static com.facebook.presto.plugin.phoenix.TypeUtils.isArrayType;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.reflect.Array.get;
import static java.lang.reflect.Array.getLength;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hbase.client.Result.getTotalSizeOfCells;
import static org.joda.time.DateTimeZone.UTC;

public class PhoenixPageSource
        implements ConnectorPageSource
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();
    private static final int ROWS_PER_REQUEST = 4096;

    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final PageBuilder pageBuilder;

    private final PhoenixConnection connection;
    private final PhoenixResultSet resultSet;

    private boolean closed;

    private long bytesRead;
    private long nanoStart;
    private long nanoEnd;

    public PhoenixPageSource(PhoenixClient phoenixClient, PhoenixSplit split, List<PhoenixColumnHandle> columns)
    {
        this.columnNames = columns.stream().map(PhoenixColumnHandle::getColumnName).collect(toList());
        this.columnTypes = columns.stream().map(PhoenixColumnHandle::getColumnType).collect(toList());
        this.pageBuilder = new PageBuilder(columnTypes);

        try {
            connection = ((PhoenixClient) phoenixClient).getConnection();

            String inputQuery = ((PhoenixClient) phoenixClient).buildSql(connection,
                    split.getCatalogName(),
                    split.getSchemaName(),
                    split.getTableName(),
                    split.getTupleDomain(),
                    columns);

            QueryPlan queryPlan = getQueryPlan(connection, inputQuery);

            Scan inputSplitScan = getInputSplit(queryPlan, split.getKeyRange());
            inputSplitScan = requireNonNull(inputSplitScan, "inputSplitScan is null");

            resultSet = getResultSet(queryPlan, inputSplitScan);
        }
        catch (Exception e) {
            throw handleSqlException(e);
        }
    }

    private Scan getInputSplit(QueryPlan queryPlan, KeyRange inputSplitKeyRange)
    {
        for (List<Scan> scans : queryPlan.getScans()) {
            for (Scan scan : scans) {
                if (KeyRange.getKeyRange(scan.getStartRow(), scan.getStopRow()).equals(inputSplitKeyRange)) {
                    return scan;
                }
            }
        }
        return null;
    }

    private PhoenixResultSet getResultSet(QueryPlan queryPlan, Scan inputSplitScan) throws Exception
    {
        List<PeekingResultIterator> iterators = new LinkedList<>();
        inputSplitScan.setAttribute(BaseScannerRegionObserver.SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));
        final TableResultIterator tableResultIterator = new TableResultIterator(
                queryPlan.getContext().getConnection().getMutationState(),
                inputSplitScan,
                null,
                queryPlan.getContext().getConnection().getQueryServices().getRenewLeaseThresholdMilliSeconds(),
                queryPlan,
                MapReduceParallelScanGrouper.getInstance());

        PeekingResultIterator peekingResultIterator = LookAheadResultIterator.wrap(tableResultIterator);
        iterators.add(peekingResultIterator);
        ResultIterator iterator = queryPlan.useRoundRobinIterator()
                ? RoundRobinResultIterator.newIterator(iterators, queryPlan)
                : ConcatResultIterator.newIterator(iterators);
        if (queryPlan.getContext().getSequenceManager().getSequenceCount() > 0) {
            iterator = new SequenceResultIterator(iterator, queryPlan.getContext()
                    .getSequenceManager());
        }

        return new PhoenixResultSet(iterator, queryPlan.getProjector()
                .cloneIfNecessary(),
                queryPlan.getContext());
    }

    @Override
    public long getCompletedBytes()
    {
        return bytesRead;
    }

    @Override
    public long getReadTimeNanos()
    {
        return nanoStart > 0L ? (nanoEnd == 0 ? System.nanoTime() : nanoEnd) - nanoStart : 0L;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return pageBuilder.getSizeInBytes();
    }

    @Override
    public boolean isFinished()
    {
        return closed && pageBuilder.isEmpty();
    }

    @Override
    public Page getNextPage()
    {
        if (nanoStart == 0) {
            nanoStart = System.nanoTime();
        }

        if (!closed) {
            try {
                for (int i = 0; i < ROWS_PER_REQUEST; i++) {
                    if (!resultSet.next()) {
                        close();
                        break;
                    }
                    bytesRead += getTotalSizeOfCells(((ResultTuple) resultSet.getCurrentRow()).getResult());

                    pageBuilder.declarePosition();
                    for (int column = 0; column < columnTypes.size(); column++) {
                        BlockBuilder output = pageBuilder.getBlockBuilder(column);
                        Type columnType = columnTypes.get(column);
                        if (isArrayType(columnType)) {
                            writeArrayBlock(output, columnType, resultSet.getArray(columnNames.get(column)));
                        }
                        else {
                            appendTo(columnType, resultSet.getObject(columnNames.get(column)), output);
                        }
                    }
                }
            }
            catch (SQLException | RuntimeException e) {
                throw handleSqlException(e);
            }
        }

        // only return a page if the buffer is full or we are finishing
        if (pageBuilder.isEmpty() || (!closed && !pageBuilder.isFull())) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();

        return page;
    }

    private void appendTo(Type type, Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                type.writeBoolean(output, (Boolean) value);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    type.writeLong(output, ((Number) value).longValue());
                }
                else if (type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
                    type.writeLong(output, ((Number) value).intValue());
                }
                else if (type.equals(RealType.REAL)) {
                    type.writeLong(output, (long) floatToRawIntBits(((Number) value).floatValue()));
                }
                else if (isShortDecimal(type)) {
                    type.writeLong(output, ((BigDecimal) value).unscaledValue().longValue());
                }
                else if (type.equals(DATE)) {
                    // JDBC returns a date using a timestamp at midnight in the JVM timezone
                    long localMillis = ((Date) value).getTime();
                    // Convert it to a midnight in UTC
                    long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, localMillis);
                    type.writeLong(output, TimeUnit.MILLISECONDS.toDays(utcMillis));
                }
                else if (type.equals(TIME)) {
                    type.writeLong(output, UTC_CHRONOLOGY.millisOfDay().get(((Date) value).getTime()));
                }
                else if (type.equals(TIMESTAMP)) {
                    type.writeLong(output, ((Date) value).getTime());
                }
                else {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for " + javaType.getSimpleName() + ":" + type.getTypeSignature());
                }
            }
            else if (javaType == double.class) {
                type.writeDouble(output, ((Number) value).doubleValue());
            }
            else if (javaType == Slice.class) {
                writeSlice(output, type, value);
            }
            else {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for " + javaType.getSimpleName() + ":" + type.getTypeSignature());
            }
        }
        catch (ClassCastException ignore) {
            // returns null instead of raising exception
            output.appendNull();
        }
    }

    private void writeSlice(BlockBuilder output, Type type, Object value)
    {
        String base = type.getTypeSignature().getBase();
        if (base.equals(StandardTypes.VARCHAR) || base.equals(StandardTypes.CHAR)) {
            type.writeSlice(output, utf8Slice((String) value));
        }
        else if (type.equals(VARBINARY)) {
            if (value instanceof byte[]) {
                type.writeSlice(output, wrappedBuffer((byte[]) value));
            }
            else {
                output.appendNull();
            }
        }
        else if (type instanceof DecimalType) {
            type.writeSlice(output, encodeScaledValue((BigDecimal) value));
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private void writeArrayBlock(BlockBuilder output, Type type, Array value)
    {
        try {
            Object[] elements = createArrayFromArrayObject(value.getArray());
            BlockBuilder builder = output.beginBlockEntry();
            Type columnType = type.getTypeParameters().get(0);
            Arrays.asList(elements).forEach(element -> appendTo(columnType, element, builder));

            output.closeEntry();
            return;
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    private Object[] createArrayFromArrayObject(Object o)
    {
        if (!o.getClass().isArray()) {
            return null;
        }

        if (!o.getClass().getComponentType().isPrimitive()) {
            return (Object[]) o;
        }

        int elementCount = getLength(o);
        Object[] elements = new Object[elementCount];

        for (int i = 0; i < elementCount; i++) {
            elements[i] = get(o, i);
        }

        return elements;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        // use try with resources to close everything properly
        try (PhoenixConnection connection = this.connection;
                ResultSet resultSet = this.resultSet) {
            // do nothing
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        nanoEnd = System.nanoTime();
    }

    private RuntimeException handleSqlException(Exception e)
    {
        try {
            close();
        }
        catch (Exception closeException) {
            // Self-suppression not permitted
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
        return new RuntimeException(e);
    }
}
