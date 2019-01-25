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
package io.prestosql.plugin.phoenix;

import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.RowBlockBuilder;
import io.prestosql.spi.block.SingleRowBlockWriter;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.RowType.Field;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.plugin.phoenix.TypeUtils.isArrayType;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.encodeScaledValue;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.reflect.Array.get;
import static java.lang.reflect.Array.getLength;
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
    private final RecordReader<NullWritable, PhoenixDBWritable> recordReader;
    private PhoenixResultSet resultSet;

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
            this.connection = phoenixClient.getConnection();
            this.recordReader = createRecordReader(phoenixClient, split, columns);
        }
        catch (Exception e) {
            throw handleSqlException(e);
        }
    }

    private RecordReader<NullWritable, PhoenixDBWritable> createRecordReader(PhoenixClient phoenixClient, PhoenixSplit split, List<PhoenixColumnHandle> columns)
            throws Exception
    {
        String inputQuery = phoenixClient.buildSql(connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                Optional.empty(),
                split.getTupleDomain(),
                columns);
        JobConf conf = new JobConf();
        phoenixClient.setJobQueryConfig(inputQuery, conf);
        PhoenixConfigurationUtil.setInputClass(conf, PhoenixDBWritable.class);
        TaskAttemptContextImpl taskContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        PhoenixInputSplit pInputSplit = split.getPhoenixInputSplit();
        RecordReader<NullWritable, PhoenixDBWritable> reader = new PhoenixInputFormat<PhoenixDBWritable>().createRecordReader(pInputSplit, taskContext);
        reader.initialize(pInputSplit, taskContext);
        return reader;
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
                    if (!hasNext()) {
                        close();
                        break;
                    }
                    bytesRead += getTotalSizeOfCells(((ResultTuple) resultSet.getCurrentRow()).getResult());

                    pageBuilder.declarePosition();
                    for (int column = 0; column < columnTypes.size(); column++) {
                        BlockBuilder output = pageBuilder.getBlockBuilder(column);
                        Type columnType = columnTypes.get(column);
                        String columnName = columnNames.get(column);
                        if (isArrayType(columnType)) {
                            writeArrayBlock(output, columnType, resultSet.getArray(columnName));
                        }
                        else if (PhoenixMetadata.isPkHandle(columnName, columnType)) {
                            writePkColumns(output, (RowType) columnType);
                        }
                        else {
                            appendTo(columnType, resultSet.getObject(columnName), output);
                        }
                    }
                }
            }
            catch (Exception e) {
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

    private void writePkColumns(BlockBuilder output, RowType rowType)
            throws SQLException
    {
        RowBlockBuilder rowBuilder = (RowBlockBuilder) output;
        List<Field> fields = rowType.getFields();
        Block[] fieldBlocks = new Block[fields.size()];
        SingleRowBlockWriter singleRowBlockWriter = rowBuilder.beginBlockEntry();
        for (int i = 0; i < fieldBlocks.length; i++) {
            Field field = fields.get(i);
            Type fieldType = field.getType();
            Object value = resultSet.getObject(field.getName().get());
            appendTo(fieldType, value, singleRowBlockWriter);
        }
        rowBuilder.closeEntry();
    }

    private boolean hasNext()
            throws IOException, InterruptedException
    {
        boolean hasNext = this.recordReader.nextKeyValue();
        if (this.resultSet == null && hasNext) {
            this.resultSet = this.recordReader.getCurrentValue().getPhoenixResultSet();
        }
        return hasNext;
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
                ResultSet resultSet = this.resultSet;
                RecordReader<NullWritable, PhoenixDBWritable> recordReader = this.recordReader) {
            // do nothing
        }
        catch (SQLException | IOException e) {
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

    private static class PhoenixDBWritable
            implements DBWritable
    {
        private PhoenixResultSet resultSet;

        @Override
        public void readFields(ResultSet rs)
                throws SQLException
        {
            if (this.resultSet == null) {
                this.resultSet = (PhoenixResultSet) rs;
            }
        }

        @Override
        public void write(PreparedStatement arg0)
                throws SQLException
        {
            throw new SQLException("Not implemented");
        }

        public PhoenixResultSet getPhoenixResultSet()
        {
            return resultSet;
        }
    }
}
