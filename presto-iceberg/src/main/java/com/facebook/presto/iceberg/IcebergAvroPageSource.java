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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static java.util.Objects.requireNonNull;

/**
 * Page source for reading Avro files in Iceberg tables.
 * Supports Avro logical types including:
 * - timestamp-micros with adjust-to-utc (maps to TIMESTAMP_WITH_TIME_ZONE)
 * - timestamp-nanos without adjust-to-utc (maps to TIMESTAMP)
 */
public class IcebergAvroPageSource
        implements ConnectorPageSource
{
    private static final int ROWS_PER_REQUEST = 8192;

    private final List<IcebergColumnHandle> columns;
    private final List<Type> types;
    private final DataFileReader<GenericRecord> dataFileReader;
    private final PageBuilder pageBuilder;
    private final RuntimeStats runtimeStats;
    private boolean closed;
    private long completedBytes;
    private long readTimeNanos;

    public IcebergAvroPageSource(
            FSDataInputStream inputStream,
            Path path,
            long start,
            long length,
            Schema fileSchema,
            List<IcebergColumnHandle> columns,
            RuntimeStats runtimeStats)
    {
        this.columns = requireNonNull(columns, "columns is null");
        this.types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .collect(ImmutableList.toImmutableList());
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
        this.pageBuilder = new PageBuilder(types);

        try {
            SeekableInput seekableInput = new HdfsSeekableInput(inputStream, path.toString());
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(fileSchema);
            this.dataFileReader = new DataFileReader<>(seekableInput, datumReader);

            // Seek to the start position if needed
            if (start > 0) {
                dataFileReader.sync(start);
            }
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, "Failed to open Avro file: " + path, e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return closed || !dataFileReader.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }

        long start = System.nanoTime();
        try {
            pageBuilder.reset();

            int rowCount = 0;
            while (rowCount < ROWS_PER_REQUEST && dataFileReader.hasNext()) {
                GenericRecord record = dataFileReader.next();
                pageBuilder.declarePosition();

                for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                    IcebergColumnHandle column = columns.get(columnIndex);
                    BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnIndex);
                    Type type = types.get(columnIndex);

                    Object value = record.get(column.getName());
                    writeValue(blockBuilder, type, value, column);
                }

                rowCount++;
                completedBytes = dataFileReader.tell();
            }

            Page page = pageBuilder.build();
            readTimeNanos += System.nanoTime() - start;
            return page;
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_BAD_DATA, "Failed to read Avro data", e);
        }
    }

    private void writeValue(BlockBuilder blockBuilder, Type type, Object value, IcebergColumnHandle column)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }

        // Handle timestamp logical types
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            // timestamp-micros with adjust-to-utc: true
            // Value is in microseconds from epoch UTC
            long micros = (Long) value;
            long millis = TimeUnit.MICROSECONDS.toMillis(micros);
            type.writeLong(blockBuilder, millis);
        }
        else if (type.equals(TIMESTAMP)) {
            // timestamp-nanos without adjust-to-utc: false
            // Value is in nanoseconds from epoch (local time)
            long nanos = (Long) value;
            long micros = TimeUnit.NANOSECONDS.toMicros(nanos);
            type.writeLong(blockBuilder, micros);
        }
        else if (value instanceof Long) {
            type.writeLong(blockBuilder, (Long) value);
        }
        else if (value instanceof Integer) {
            type.writeLong(blockBuilder, ((Integer) value).longValue());
        }
        else if (value instanceof Double) {
            type.writeDouble(blockBuilder, (Double) value);
        }
        else if (value instanceof Float) {
            type.writeLong(blockBuilder, Float.floatToIntBits((Float) value));
        }
        else if (value instanceof Boolean) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (value instanceof CharSequence) {
            type.writeSlice(blockBuilder, io.airlift.slice.Slices.utf8Slice(value.toString()));
        }
        else if (value instanceof java.nio.ByteBuffer) {
            type.writeSlice(blockBuilder, io.airlift.slice.Slices.wrappedBuffer((java.nio.ByteBuffer) value));
        }
        else {
            throw new PrestoException(ICEBERG_BAD_DATA, "Unsupported Avro type: " + value.getClass());
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public long getCompletedPositions()
    {
        return 0;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            dataFileReader.close();
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, "Failed to close Avro reader", e);
        }
    }

    /**
     * Adapter to make FSDataInputStream work with Avro's SeekableInput interface
     */
    private static class HdfsSeekableInput
            implements SeekableInput
    {
        private final FSDataInputStream inputStream;
        private final String path;

        public HdfsSeekableInput(FSDataInputStream inputStream, String path)
        {
            this.inputStream = requireNonNull(inputStream, "inputStream is null");
            this.path = requireNonNull(path, "path is null");
        }

        @Override
        public void seek(long position)
                throws IOException
        {
            inputStream.seek(position);
        }

        @Override
        public long tell()
                throws IOException
        {
            return inputStream.getPos();
        }

        @Override
        public long length()
                throws IOException
        {
            return inputStream.available();
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException
        {
            return inputStream.read(b, off, len);
        }

        @Override
        public void close()
                throws IOException
        {
            inputStream.close();
        }
    }
}
