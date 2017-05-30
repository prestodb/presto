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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.Ranges;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static com.facebook.presto.raptorx.RaptorColumnHandle.isBucketNumberColumn;
import static com.facebook.presto.raptorx.metadata.IndexWriter.bindValue;
import static com.facebook.presto.raptorx.metadata.IndexWriter.jdbcType;
import static com.facebook.presto.raptorx.metadata.IndexWriter.maxColumn;
import static com.facebook.presto.raptorx.metadata.IndexWriter.minColumn;
import static com.facebook.presto.raptorx.util.FloatingPointUtil.doubleToSortableLong;
import static com.facebook.presto.raptorx.util.FloatingPointUtil.floatToSortableInt;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class ChunkPredicate
{
    private final String predicate;
    private final List<JDBCType> types;
    private final List<Object> values;

    private ChunkPredicate(String predicate, List<JDBCType> types, List<Object> values)
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

    public void bind(PreparedStatement statement, int start)
            throws SQLException
    {
        for (int i = 0; i < types.size(); i++) {
            JDBCType type = types.get(i);
            Object value = values.get(i);
            bindValue(statement, type, value, i + start);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(predicate)
                .toString();
    }

    public static ChunkPredicate create(TupleDomain<Long> tupleDomain)
    {
        Map<Long, Domain> domains = tupleDomain.getDomains()
                .orElseThrow(() -> new IllegalArgumentException("no domains"));

        StringJoiner predicate = new StringJoiner(" AND ").setEmptyValue("true");
        ImmutableList.Builder<JDBCType> types = ImmutableList.builder();
        ImmutableList.Builder<Object> values = ImmutableList.builder();

        domains.forEach((columnId, domain) -> {
            if (domain.isNullAllowed() || domain.isAll()) {
                return;
            }

            JDBCType jdbcType = jdbcType(domain.getType());
            if (jdbcType == null) {
                return;
            }

            Ranges ranges = domain.getValues().getRanges();

            // TODO: support multiple ranges
            if (ranges.getRangeCount() != 1) {
                return;
            }
            Range range = getOnlyElement(ranges.getOrderedRanges());

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
            if (isBucketNumberColumn(columnId)) {
                min = "bucket_number";
                max = "bucket_number";
            }
            else {
                min = minColumn(columnId);
                max = maxColumn(columnId);
            }

            if (minValue != null) {
                predicate.add(max + " >= ?");
                types.add(jdbcType);
                values.add(convertValue(minValue, domain.getType()));
            }
            if (maxValue != null) {
                predicate.add(min + " <= ?");
                types.add(jdbcType);
                values.add(convertValue(maxValue, domain.getType()));
            }
        });

        return new ChunkPredicate(predicate.toString(), types.build(), values.build());
    }

    private static Object convertValue(Object value, Type type)
    {
        if (value instanceof Slice) {
            return ((Slice) value).getBytes();
        }

        if (type.equals(DOUBLE)) {
            return doubleToSortableLong(((Number) value).doubleValue());
        }
        if (type.equals(REAL)) {
            return floatToSortableInt(((Number) value).floatValue());
        }

        return value;
    }
}
