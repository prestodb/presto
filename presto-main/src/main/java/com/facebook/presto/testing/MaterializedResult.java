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
package com.facebook.presto.testing;

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.SqlTime;
import com.facebook.presto.common.type.SqlTimeWithTimeZone;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoWarning;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MaterializedResult
        implements Iterable<MaterializedRow>
{
    public static final int DEFAULT_PRECISION = 5;

    private final List<MaterializedRow> rows;
    private final List<Type> types;
    private final Map<String, String> setSessionProperties;
    private final Set<String> resetSessionProperties;
    private final Optional<String> updateType;
    private final OptionalLong updateCount;
    private final List<PrestoWarning> warnings;

    public MaterializedResult(List<MaterializedRow> rows, List<? extends Type> types)
    {
        this(rows, types, ImmutableMap.of(), ImmutableSet.of(), Optional.empty(), OptionalLong.empty(), ImmutableList.of());
    }

    public MaterializedResult(
            List<MaterializedRow> rows,
            List<? extends Type> types,
            Map<String, String> setSessionProperties,
            Set<String> resetSessionProperties,
            Optional<String> updateType,
            OptionalLong updateCount,
            List<PrestoWarning> warnings)
    {
        this.rows = ImmutableList.copyOf(requireNonNull(rows, "rows is null"));
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.setSessionProperties = ImmutableMap.copyOf(requireNonNull(setSessionProperties, "setSessionProperties is null"));
        this.resetSessionProperties = ImmutableSet.copyOf(requireNonNull(resetSessionProperties, "resetSessionProperties is null"));
        this.updateType = requireNonNull(updateType, "updateType is null");
        this.updateCount = requireNonNull(updateCount, "updateCount is null");
        this.warnings = requireNonNull(warnings, "warnings is null");
    }

    public int getRowCount()
    {
        return rows.size();
    }

    @Override
    public Iterator<MaterializedRow> iterator()
    {
        return rows.iterator();
    }

    public List<MaterializedRow> getMaterializedRows()
    {
        return rows;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    public Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    public Optional<String> getUpdateType()
    {
        return updateType;
    }

    public OptionalLong getUpdateCount()
    {
        return updateCount;
    }

    public List<PrestoWarning> getWarnings()
    {
        return warnings;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MaterializedResult o = (MaterializedResult) obj;
        return Objects.equals(types, o.types) &&
                Objects.equals(rows, o.rows) &&
                Objects.equals(setSessionProperties, o.setSessionProperties) &&
                Objects.equals(resetSessionProperties, o.resetSessionProperties) &&
                Objects.equals(updateType, o.updateType) &&
                Objects.equals(updateCount, o.updateCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rows, types, setSessionProperties, resetSessionProperties, updateType, updateCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rows", rows)
                .add("types", types)
                .add("setSessionProperties", setSessionProperties)
                .add("resetSessionProperties", resetSessionProperties)
                .add("updateType", updateType.orElse(null))
                .add("updateCount", updateCount.isPresent() ? updateCount.getAsLong() : null)
                .omitNullValues()
                .toString();
    }

    public Stream<Object> getOnlyColumn()
    {
        checkState(types.size() == 1, "result set must have exactly one column");
        return rows.stream()
                .map(row -> row.getField(0));
    }

    public Set<Object> getOnlyColumnAsSet()
    {
        return getOnlyColumn().collect(toImmutableSet());
    }

    public Object getOnlyValue()
    {
        checkState(rows.size() == 1, "result set must have exactly one row");
        checkState(types.size() == 1, "result set must have exactly one column");
        return rows.get(0).getField(0);
    }

    public Page toPage()
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        for (MaterializedRow row : rows) {
            appendToPage(pageBuilder, row);
        }
        return pageBuilder.build();
    }

    private static void appendToPage(PageBuilder pageBuilder, MaterializedRow row)
    {
        for (int field = 0; field < row.getFieldCount(); field++) {
            Type type = pageBuilder.getType(field);
            Object value = row.getField(field);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(field);
            writeValue(type, blockBuilder, value);
        }
        pageBuilder.declarePosition();
    }

    private static void writeValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (BIGINT.equals(type)) {
            type.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (INTEGER.equals(type)) {
            type.writeLong(blockBuilder, ((Number) value).intValue());
        }
        else if (SMALLINT.equals(type)) {
            type.writeLong(blockBuilder, ((Number) value).shortValue());
        }
        else if (TINYINT.equals(type)) {
            type.writeLong(blockBuilder, ((Number) value).byteValue());
        }
        else if (REAL.equals(type)) {
            type.writeLong(blockBuilder, (long) floatToRawIntBits(((Number) value).floatValue()));
        }
        else if (DOUBLE.equals(type)) {
            type.writeDouble(blockBuilder, ((Number) value).doubleValue());
        }
        else if (BOOLEAN.equals(type)) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (type instanceof VarcharType) {
            type.writeSlice(blockBuilder, Slices.utf8Slice((String) value));
        }
        else if (type instanceof CharType) {
            type.writeSlice(blockBuilder, Slices.utf8Slice((String) value));
        }
        else if (VARBINARY.equals(type)) {
            type.writeSlice(blockBuilder, Slices.wrappedBuffer((byte[]) value));
        }
        else if (DATE.equals(type)) {
            int days = ((SqlDate) value).getDays();
            type.writeLong(blockBuilder, days);
        }
        else if (TIME.equals(type)) {
            SqlTime time = (SqlTime) value;
            if (time.isLegacyTimestamp()) {
                type.writeLong(blockBuilder, time.getMillisUtc());
            }
            else {
                type.writeLong(blockBuilder, time.getMillis());
            }
        }
        else if (TIME_WITH_TIME_ZONE.equals(type)) {
            long millisUtc = ((SqlTimeWithTimeZone) value).getMillisUtc();
            TimeZoneKey timeZoneKey = ((SqlTimeWithTimeZone) value).getTimeZoneKey();
            type.writeLong(blockBuilder, packDateTimeWithZone(millisUtc, timeZoneKey));
        }
        else if (TIMESTAMP.equals(type)) {
            long millisUtc = ((SqlTimestamp) value).getMillisUtc();
            type.writeLong(blockBuilder, millisUtc);
        }
        else if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            long millisUtc = ((SqlTimestampWithTimeZone) value).getMillisUtc();
            TimeZoneKey timeZoneKey = ((SqlTimestampWithTimeZone) value).getTimeZoneKey();
            type.writeLong(blockBuilder, packDateTimeWithZone(millisUtc, timeZoneKey));
        }
        else if (ARRAY.equals(type.getTypeSignature().getBase())) {
            List<Object> list = (List<Object>) value;
            Type elementType = ((ArrayType) type).getElementType();
            BlockBuilder arrayBlockBuilder = blockBuilder.beginBlockEntry();
            for (Object element : list) {
                writeValue(elementType, arrayBlockBuilder, element);
            }
            blockBuilder.closeEntry();
        }
        else if (MAP.equals(type.getTypeSignature().getBase())) {
            Map<Object, Object> map = (Map<Object, Object>) value;
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();
            BlockBuilder mapBlockBuilder = blockBuilder.beginBlockEntry();
            for (Entry<Object, Object> entry : map.entrySet()) {
                writeValue(keyType, mapBlockBuilder, entry.getKey());
                writeValue(valueType, mapBlockBuilder, entry.getValue());
            }
            blockBuilder.closeEntry();
        }
        else if (type instanceof RowType) {
            List<Object> row = (List<Object>) value;
            List<Type> fieldTypes = type.getTypeParameters();
            BlockBuilder rowBlockBuilder = blockBuilder.beginBlockEntry();
            for (int field = 0; field < row.size(); field++) {
                writeValue(fieldTypes.get(field), rowBlockBuilder, row.get(field));
            }
            blockBuilder.closeEntry();
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    /**
     * Converts this {@link MaterializedResult} to a new one, representing the data using the same type domain as returned by {@code TestingPrestoClient}.
     */
    public MaterializedResult toTestTypes()
    {
        return new MaterializedResult(
                rows.stream()
                        .map(MaterializedResult::convertToTestTypes)
                        .collect(toImmutableList()),
                types,
                setSessionProperties,
                resetSessionProperties,
                updateType,
                updateCount,
                warnings);
    }

    private static MaterializedRow convertToTestTypes(MaterializedRow prestoRow)
    {
        List<Object> convertedValues = new ArrayList<>();
        for (int field = 0; field < prestoRow.getFieldCount(); field++) {
            Object prestoValue = prestoRow.getField(field);
            Object convertedValue;
            if (prestoValue instanceof SqlDate) {
                convertedValue = LocalDate.ofEpochDay(((SqlDate) prestoValue).getDays());
            }
            else if (prestoValue instanceof SqlTime) {
                convertedValue = DateTimeFormatter.ISO_LOCAL_TIME.parse(prestoValue.toString(), LocalTime::from);
            }
            else if (prestoValue instanceof SqlTimeWithTimeZone) {
                // Political timezone cannot be represented in OffsetTime and there isn't any better representation.
                long millisUtc = ((SqlTimeWithTimeZone) prestoValue).getMillisUtc();
                ZoneOffset zone = toZoneOffset(((SqlTimeWithTimeZone) prestoValue).getTimeZoneKey());
                convertedValue = OffsetTime.of(
                        LocalTime.ofNanoOfDay(MILLISECONDS.toNanos(millisUtc) + SECONDS.toNanos(zone.getTotalSeconds())),
                        zone);
            }
            else if (prestoValue instanceof SqlTimestamp) {
                convertedValue = SqlTimestamp.JSON_FORMATTER.parse(prestoValue.toString(), LocalDateTime::from);
            }
            else if (prestoValue instanceof SqlTimestampWithTimeZone) {
                convertedValue = Instant.ofEpochMilli(((SqlTimestampWithTimeZone) prestoValue).getMillisUtc())
                        .atZone(ZoneId.of(((SqlTimestampWithTimeZone) prestoValue).getTimeZoneKey().getId()));
            }
            else if (prestoValue instanceof SqlDecimal) {
                convertedValue = ((SqlDecimal) prestoValue).toBigDecimal();
            }
            else {
                convertedValue = prestoValue;
            }
            convertedValues.add(convertedValue);
        }
        return new MaterializedRow(prestoRow.getPrecision(), convertedValues);
    }

    private static ZoneOffset toZoneOffset(TimeZoneKey timeZoneKey)
    {
        requireNonNull(timeZoneKey, "timeZoneKey is null");
        if (Objects.equals("UTC", timeZoneKey.getId())) {
            return ZoneOffset.UTC;
        }

        checkArgument(timeZoneKey.getId().matches("[+-]\\d\\d:\\d\\d"), "Not a zone-offset timezone: %s", timeZoneKey);
        return ZoneOffset.of(timeZoneKey.getId());
    }

    public static MaterializedResult materializeSourceDataStream(Session session, ConnectorPageSource pageSource, List<Type> types)
    {
        return materializeSourceDataStream(session.toConnectorSession(), pageSource, types);
    }

    public static MaterializedResult materializeSourceDataStream(ConnectorSession session, ConnectorPageSource pageSource, List<Type> types)
    {
        MaterializedResult.Builder builder = resultBuilder(session, types);
        while (!pageSource.isFinished()) {
            Page outputPage = pageSource.getNextPage();
            if (outputPage == null || outputPage.getPositionCount() == 0) {
                continue;
            }
            builder.page(outputPage);
        }
        return builder.build();
    }

    public static Builder resultBuilder(Session session, Type... types)
    {
        return resultBuilder(session.toConnectorSession(), types);
    }

    public static Builder resultBuilder(Session session, Iterable<? extends Type> types)
    {
        return resultBuilder(session.toConnectorSession(), types);
    }

    public static Builder resultBuilder(ConnectorSession session, Type... types)
    {
        return resultBuilder(session, ImmutableList.copyOf(types));
    }

    public static Builder resultBuilder(ConnectorSession session, Iterable<? extends Type> types)
    {
        return new Builder(session, ImmutableList.copyOf(types));
    }

    public static class Builder
    {
        private final ConnectorSession session;
        private final List<Type> types;
        private final ImmutableList.Builder<MaterializedRow> rows = ImmutableList.builder();

        Builder(ConnectorSession session, List<Type> types)
        {
            this.session = session;
            this.types = ImmutableList.copyOf(types);
        }

        public synchronized Builder rows(List<MaterializedRow> rows)
        {
            this.rows.addAll(rows);
            return this;
        }

        public synchronized Builder row(Object... values)
        {
            rows.add(new MaterializedRow(DEFAULT_PRECISION, values));
            return this;
        }

        public synchronized Builder rows(Object[][] rows)
        {
            for (Object[] row : rows) {
                row(row);
            }
            return this;
        }

        public synchronized Builder pages(Iterable<Page> pages)
        {
            for (Page page : pages) {
                this.page(page);
            }

            return this;
        }

        public synchronized Builder page(Page page)
        {
            requireNonNull(page, "page is null");
            checkArgument(page.getChannelCount() == types.size(), "Expected a page with %s columns, but got %s columns", types.size(), page.getChannelCount());

            for (int position = 0; position < page.getPositionCount(); position++) {
                List<Object> values = new ArrayList<>(page.getChannelCount());
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Type type = types.get(channel);
                    Block block = page.getBlock(channel);
                    values.add(type.getObjectValue(session.getSqlFunctionProperties(), block, position));
                }
                values = Collections.unmodifiableList(values);

                rows.add(new MaterializedRow(DEFAULT_PRECISION, values));
            }
            return this;
        }

        public synchronized MaterializedResult build()
        {
            return new MaterializedResult(rows.build(), types);
        }
    }
}
