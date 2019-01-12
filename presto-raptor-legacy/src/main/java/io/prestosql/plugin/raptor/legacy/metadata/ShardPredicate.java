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
package io.prestosql.plugin.raptor.legacy.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.plugin.raptor.legacy.RaptorColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.Ranges;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringJoiner;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.raptor.legacy.metadata.DatabaseShardManager.maxColumn;
import static io.prestosql.plugin.raptor.legacy.metadata.DatabaseShardManager.minColumn;
import static io.prestosql.plugin.raptor.legacy.storage.ColumnIndexStatsUtils.jdbcType;
import static io.prestosql.plugin.raptor.legacy.storage.ShardStats.truncateIndexValue;
import static io.prestosql.plugin.raptor.legacy.util.UuidUtil.uuidStringToBytes;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class ShardPredicate
{
    private final String predicate;
    private final List<JDBCType> types;
    private final List<Object> values;
    private static final int MAX_RANGE_COUNT = 100;

    private ShardPredicate(String predicate, List<JDBCType> types, List<Object> values)
    {
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.values = ImmutableList.copyOf(requireNonNull(values, "values is null"));
        checkArgument(types.size() == values.size(), "types and values sizes do not match");
    }

    public String getPredicate()
    {
        return predicate;
    }

    public void bind(PreparedStatement statement)
            throws SQLException
    {
        for (int i = 0; i < types.size(); i++) {
            JDBCType type = types.get(i);
            Object value = values.get(i);
            bindValue(statement, type, value, i + 1);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(predicate)
                .toString();
    }

    public static ShardPredicate create(TupleDomain<RaptorColumnHandle> tupleDomain)
    {
        StringJoiner predicate = new StringJoiner(" AND ").setEmptyValue("true");
        ImmutableList.Builder<JDBCType> types = ImmutableList.builder();
        ImmutableList.Builder<Object> values = ImmutableList.builder();

        for (Entry<RaptorColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
            Domain domain = entry.getValue();
            if (domain.isNullAllowed() || domain.isAll()) {
                continue;
            }
            RaptorColumnHandle handle = entry.getKey();
            Type type = handle.getColumnType();

            JDBCType jdbcType = jdbcType(type);
            if (jdbcType == null) {
                continue;
            }

            if (handle.isShardUuid()) {
                predicate.add(createShardPredicate(types, values, domain, jdbcType));
                continue;
            }

            if (!domain.getType().isOrderable()) {
                continue;
            }

            StringJoiner columnPredicate = new StringJoiner(" OR ", "(", ")").setEmptyValue("true");
            Ranges ranges = domain.getValues().getRanges();

            // prevent generating complicated metadata queries
            if (ranges.getRangeCount() > MAX_RANGE_COUNT) {
                continue;
            }

            for (Range range : ranges.getOrderedRanges()) {
                Object minValue = null;
                Object maxValue = null;
                if (range.isSingleValue()) {
                    minValue = range.getSingleValue();
                    maxValue = range.getSingleValue();
                }
                else {
                    if (!range.getLow().isLowerUnbounded()) {
                        minValue = range.getLow().getValue();
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        maxValue = range.getHigh().getValue();
                    }
                }

                String min;
                String max;
                if (handle.isBucketNumber()) {
                    min = "bucket_number";
                    max = "bucket_number";
                }
                else {
                    min = minColumn(handle.getColumnId());
                    max = maxColumn(handle.getColumnId());
                }

                StringJoiner rangePredicate = new StringJoiner(" AND ", "(", ")").setEmptyValue("true");
                if (minValue != null) {
                    rangePredicate.add(format("(%s >= ? OR %s IS NULL)", max, max));
                    types.add(jdbcType);
                    values.add(minValue);
                }
                if (maxValue != null) {
                    rangePredicate.add(format("(%s <= ? OR %s IS NULL)", min, min));
                    types.add(jdbcType);
                    values.add(maxValue);
                }
                columnPredicate.add(rangePredicate.toString());
            }
            predicate.add(columnPredicate.toString());
        }
        return new ShardPredicate(predicate.toString(), types.build(), values.build());
    }

    private static String createShardPredicate(ImmutableList.Builder<JDBCType> types, ImmutableList.Builder<Object> values, Domain domain, JDBCType jdbcType)
    {
        List<Range> ranges = domain.getValues().getRanges().getOrderedRanges();

        // only apply predicates if all ranges are single values
        if (ranges.isEmpty() || !ranges.stream().allMatch(Range::isSingleValue)) {
            return "true";
        }

        ImmutableList.Builder<Object> valuesBuilder = ImmutableList.builder();
        ImmutableList.Builder<JDBCType> typesBuilder = ImmutableList.builder();

        StringJoiner rangePredicate = new StringJoiner(" OR ");
        for (Range range : ranges) {
            Slice uuidText = (Slice) range.getSingleValue();
            try {
                Slice uuidBytes = uuidStringToBytes(uuidText);
                typesBuilder.add(jdbcType);
                valuesBuilder.add(uuidBytes);
            }
            catch (IllegalArgumentException e) {
                return "true";
            }
            rangePredicate.add("shard_uuid = ?");
        }

        types.addAll(typesBuilder.build());
        values.addAll(valuesBuilder.build());
        return rangePredicate.toString();
    }

    @VisibleForTesting
    protected List<JDBCType> getTypes()
    {
        return types;
    }

    @VisibleForTesting
    protected List<Object> getValues()
    {
        return values;
    }

    public static void bindValue(PreparedStatement statement, JDBCType type, Object value, int index)
            throws SQLException
    {
        if (value == null) {
            statement.setNull(index, type.getVendorTypeNumber());
            return;
        }

        switch (type) {
            case BOOLEAN:
                statement.setBoolean(index, (boolean) value);
                return;
            case INTEGER:
                statement.setInt(index, ((Number) value).intValue());
                return;
            case BIGINT:
                statement.setLong(index, ((Number) value).longValue());
                return;
            case DOUBLE:
                statement.setDouble(index, ((Number) value).doubleValue());
                return;
            case VARBINARY:
                statement.setBytes(index, truncateIndexValue((Slice) value).getBytes());
                return;
        }
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type: " + type);
    }
}
