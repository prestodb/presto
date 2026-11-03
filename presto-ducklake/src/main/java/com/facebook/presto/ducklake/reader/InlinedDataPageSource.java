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
package com.facebook.presto.ducklake.reader;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.catalog.DuckLakeInlinedRowSource;
import com.facebook.presto.ducklake.split.pruning.StatsValueParser;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Time;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DATA;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Reads the rows of one inlined table (spec &sect;3, DuckLake's "data inlining") off a {@link
 * DuckLakeInlinedRowSource} opened by {@link
 * com.facebook.presto.ducklake.catalog.DuckLakeCatalog#openInlinedRows}, converting each JDBC
 * value to a Presto block by the requested column's Presto type. A row's {@code $row_id} comes
 * from {@link DuckLakeInlinedRowSource#getRowId()}; {@code $path} and {@code $row_position} (an
 * inlined row lives in the catalog database, not in a Parquet file, so neither is meaningful) are
 * always null. Because a PostgreSQL catalog stores an inlined column under a different physical
 * type than the DuckLake column's own type (see {@link #checkSupported} and {@link #writeValue}
 * for the mapping, taken from the extension's {@code PostgresMetadataManager}), only a fixed set
 * of primitive DuckLake types can be converted; {@link #regularColumnNames} validates every
 * requested regular column against that set before the caller ever asks the catalog to open a
 * connection, and the constructor repeats the same check so it also protects direct callers (for
 * example, tests) that skip {@link #regularColumnNames}.
 */
public class InlinedDataPageSource
        implements ConnectorPageSource
{
    public static final int DEFAULT_BATCH_SIZE = 1024;

    private final DuckLakeInlinedRowSource rowSource;
    private final List<ChannelReader> readers;
    private final int batchSize;
    private long completedPositions;
    private long retainedSizeInBytes;
    private boolean finished;

    public InlinedDataPageSource(DuckLakeInlinedRowSource rowSource, List<DuckLakeColumnHandle> columns, int batchSize)
    {
        this.rowSource = requireNonNull(rowSource, "rowSource is null");
        requireNonNull(columns, "columns is null");
        checkArgument(batchSize > 0, "batchSize must be positive: %s", batchSize);
        this.batchSize = batchSize;

        ImmutableList.Builder<ChannelReader> readers = ImmutableList.builder();
        int regularIndex = 0;
        for (DuckLakeColumnHandle column : columns) {
            if (column.isRowIdColumn()) {
                readers.add(rowIdReader());
            }
            else if (column.isPathColumn() || column.isRowPositionColumn()) {
                readers.add(nullReader(column.getType()));
            }
            else {
                checkSupported(column);
                readers.add(regularReader(column, regularIndex));
                regularIndex++;
            }
        }
        this.readers = readers.build();
    }

    /**
     * The names, in request order, of the regular (non-metadata) columns of {@code columns} —
     * exactly the {@code columnNames} list to pass to {@link
     * com.facebook.presto.ducklake.catalog.DuckLakeCatalog#openInlinedRows}. Validates every
     * regular column first, so an unsupported column is rejected before a catalog connection is
     * ever opened.
     */
    public static List<String> regularColumnNames(List<DuckLakeColumnHandle> columns)
    {
        ImmutableList.Builder<String> names = ImmutableList.builder();
        for (DuckLakeColumnHandle column : columns) {
            if (column.isRowIdColumn() || column.isPathColumn() || column.isRowPositionColumn()) {
                continue;
            }
            checkSupported(column);
            names.add(column.getName());
        }
        return names.build();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }
        int channelCount = readers.size();
        BlockBuilder[] builders = new BlockBuilder[channelCount];
        for (int channel = 0; channel < channelCount; channel++) {
            builders[channel] = readers.get(channel).getType().createBlockBuilder(null, batchSize);
        }

        int count = 0;
        while (count < batchSize) {
            if (!rowSource.advanceNextRow()) {
                finished = true;
                break;
            }
            for (int channel = 0; channel < channelCount; channel++) {
                readers.get(channel).read(builders[channel], rowSource);
            }
            count++;
        }
        if (count == 0) {
            return null;
        }
        completedPositions += count;

        Block[] blocks = new Block[channelCount];
        long retainedSize = 0;
        for (int channel = 0; channel < channelCount; channel++) {
            blocks[channel] = builders[channel].build();
            retainedSize += blocks[channel].getRetainedSizeInBytes();
        }
        retainedSizeInBytes = retainedSize;
        return new Page(count, blocks);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void close()
            throws IOException
    {
        rowSource.close();
    }

    private static void checkSupported(DuckLakeColumnHandle column)
    {
        if (column.getColumnIdentity().getTypeCategory() != PRIMITIVE || !isSupportedType(column.getType())) {
            throw new PrestoException(
                    DUCKLAKE_UNSUPPORTED_FEATURE,
                    format("Reading inlined data of type '%s' for column '%s' is not supported", column.getType(), column.getName()));
        }
    }

    private static boolean isSupportedType(Type type)
    {
        return type instanceof BooleanType ||
                type instanceof TinyintType ||
                type instanceof SmallintType ||
                type instanceof IntegerType ||
                type instanceof BigintType ||
                type instanceof RealType ||
                type instanceof DoubleType ||
                type instanceof DecimalType ||
                type instanceof VarcharType ||
                type instanceof JsonType ||
                type instanceof VarbinaryType ||
                type instanceof DateType ||
                type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType ||
                type instanceof TimeType ||
                type instanceof UuidType;
    }

    private static ChannelReader rowIdReader()
    {
        return new ChannelReader()
        {
            @Override
            public Type getType()
            {
                return BIGINT;
            }

            @Override
            public void read(BlockBuilder builder, DuckLakeInlinedRowSource source)
            {
                BIGINT.writeLong(builder, source.getRowId());
            }
        };
    }

    private static ChannelReader nullReader(Type type)
    {
        return new ChannelReader()
        {
            @Override
            public Type getType()
            {
                return type;
            }

            @Override
            public void read(BlockBuilder builder, DuckLakeInlinedRowSource source)
            {
                builder.appendNull();
            }
        };
    }

    private static ChannelReader regularReader(DuckLakeColumnHandle column, int columnIndex)
    {
        Type type = column.getType();
        String duckLakeType = column.getColumnIdentity().getDuckLakeType();
        return new ChannelReader()
        {
            @Override
            public Type getType()
            {
                return type;
            }

            @Override
            public void read(BlockBuilder builder, DuckLakeInlinedRowSource source)
            {
                writeValue(builder, type, duckLakeType, source.getObject(columnIndex));
            }
        };
    }

    /**
     * Converts one JDBC value into {@code type}'s native block representation, dispatching
     * entirely on {@code type} to match the PostgreSQL storage types documented in this class's
     * javadoc: {@code varchar}/{@code blob} arrive as {@code byte[]} (BYTEA); {@code date}/every
     * {@code timestamp*} variant/{@code timestamptz} arrive as DuckDB literal text, parsed by
     * {@link StatsValueParser#parseMin} using {@code duckLakeType} and {@code type}; {@code int8}
     * arrives as a JDBC {@code Short} (Postgres {@code smallint}); {@code uint8}/{@code uint16}
     * arrive as a JDBC {@code Integer}; {@code uint32} arrives as a JDBC {@code Long}; {@code
     * float32}/{@code float64} arrive as {@code Float}/{@code Double}; every other supported type
     * keeps its natural PostgreSQL/JDBC representation.
     */
    private static void writeValue(BlockBuilder builder, Type type, String duckLakeType, Object value)
    {
        if (value == null) {
            builder.appendNull();
            return;
        }
        if (type instanceof BooleanType) {
            type.writeBoolean(builder, (Boolean) value);
        }
        else if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType) {
            type.writeLong(builder, ((Number) value).longValue());
        }
        else if (type instanceof RealType) {
            type.writeLong(builder, Float.floatToIntBits(((Number) value).floatValue()));
        }
        else if (type instanceof DoubleType) {
            type.writeDouble(builder, ((Number) value).doubleValue());
        }
        else if (type instanceof DecimalType) {
            writeDecimal(builder, (DecimalType) type, (BigDecimal) value);
        }
        else if (type instanceof VarcharType || type instanceof JsonType) {
            type.writeSlice(builder, toVarcharSlice(value));
        }
        else if (type instanceof VarbinaryType) {
            type.writeSlice(builder, Slices.wrappedBuffer((byte[]) value));
        }
        else if (type instanceof DateType || type instanceof TimestampType || type instanceof TimestampWithTimeZoneType) {
            type.writeLong(builder, parseDateOrTimestamp(duckLakeType, type, (String) value));
        }
        else if (type instanceof TimeType) {
            type.writeLong(builder, timeMillis(value));
        }
        else if (type instanceof UuidType) {
            type.writeSlice(builder, toUuidSlice(value));
        }
        else {
            // Unreachable: checkSupported rejects every other type before a reader is built.
            throw new PrestoException(DUCKLAKE_UNSUPPORTED_FEATURE, format("Reading inlined data of type '%s' is not supported", type));
        }
    }

    private static void writeDecimal(BlockBuilder builder, DecimalType type, BigDecimal value)
    {
        BigDecimal scaled = value.setScale(type.getScale(), RoundingMode.UNNECESSARY);
        if (Decimals.isShortDecimal(type)) {
            type.writeLong(builder, scaled.unscaledValue().longValueExact());
        }
        else {
            type.writeSlice(builder, Decimals.encodeScaledValue(scaled, type.getScale()));
        }
    }

    private static Slice toVarcharSlice(Object value)
    {
        if (value instanceof byte[]) {
            return Slices.wrappedBuffer((byte[]) value);
        }
        if (value instanceof String) {
            return Slices.utf8Slice((String) value);
        }
        throw new PrestoException(DUCKLAKE_BAD_DATA, "Unexpected inlined varchar/json value type: " + value.getClass());
    }

    private static long parseDateOrTimestamp(String duckLakeType, Type type, String value)
    {
        return (Long) StatsValueParser.parseMin(duckLakeType, type, value)
                .orElseThrow(() -> new PrestoException(DUCKLAKE_BAD_DATA, format("Could not parse inlined %s value: %s", type, value)));
    }

    private static long timeMillis(Object value)
    {
        LocalTime localTime;
        if (value instanceof LocalTime) {
            localTime = (LocalTime) value;
        }
        else if (value instanceof Time) {
            localTime = ((Time) value).toLocalTime();
        }
        else {
            throw new PrestoException(DUCKLAKE_BAD_DATA, "Unexpected inlined time value type: " + value.getClass());
        }
        return localTime.toNanoOfDay() / 1_000_000L;
    }

    private static Slice toUuidSlice(Object value)
    {
        if (value instanceof UUID) {
            return UuidType.javaUuidToPrestoUuid((UUID) value);
        }
        if (value instanceof String) {
            return UuidType.javaUuidToPrestoUuid(UUID.fromString((String) value));
        }
        throw new PrestoException(DUCKLAKE_BAD_DATA, "Unexpected inlined uuid value type: " + value.getClass());
    }

    /**
     * Produces one output channel's block, one row at a time, out of the {@link
     * DuckLakeInlinedRowSource}'s current row.
     */
    private interface ChannelReader
    {
        Type getType();

        void read(BlockBuilder builder, DuckLakeInlinedRowSource source);
    }
}
