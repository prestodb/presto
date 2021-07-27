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
package com.facebook.presto.maxcompute.util;

import com.aliyun.odps.PartitionSpec;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.maxcompute.MaxComputeColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.hive.HiveUtil.parsePartitionValue;
import static com.facebook.presto.hive.metastore.MetastoreUtil.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;

public class MaxComputePredicateUtils
{
    private MaxComputePredicateUtils()
    {
    }

    public static boolean matchPartition(PartitionSpec partSpec, TupleDomain<ColumnHandle> tupleDomain, List<MaxComputeColumnHandle> partitionColumns, Predicate<Map<ColumnHandle, NullableValue>> predicate)
    {
        ImmutableMap.Builder<ColumnHandle, NullableValue> partitionBuilder = ImmutableMap.builder();
        for (MaxComputeColumnHandle columnHandle : partitionColumns) {
            partitionBuilder.put(columnHandle, parsePartitionValue(partSpec.toString(),
                    getPartitionValueOrElse(partSpec, columnHandle.getColumnName(), HIVE_DEFAULT_DYNAMIC_PARTITION), columnHandle.getColumnType(), DateTimeZone.getDefault()));
        }
        Map<ColumnHandle, NullableValue> partition = partitionBuilder.build();
        Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().get();
        for (MaxComputeColumnHandle column : partitionColumns) {
            NullableValue value = partition.get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }
        return predicate.test(partition);
    }

    public static Predicate<Map<ColumnHandle, NullableValue>> convertToPredicate(TupleDomain<ColumnHandle> tupleDomain)
    {
        return bindings -> tupleDomain.contains(TupleDomain.fromFixedValues(bindings));
    }

    public static Optional<Pair<PartitionSpec, PartitionSpec>> convertToPartitionSpecRange(
            TupleDomain<ColumnHandle> tupleDomain, List<MaxComputeColumnHandle> partitionColumns)
    {
        Optional<List<ColumnDomain<ColumnHandle>>> cdm = tupleDomain.getColumnDomains();
        if (!cdm.isPresent()) {
            return Optional.empty();
        }

        //pick up single values and single ranges
        ImmutableMap.Builder<MaxComputeColumnHandle, String> singleValuesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<MaxComputeColumnHandle, List<Range>> rangesBuilder = ImmutableMap.builder();
        List<ColumnDomain<ColumnHandle>> cl = cdm.get();
        for (ColumnDomain<ColumnHandle> c : cl) {
            MaxComputeColumnHandle column = (MaxComputeColumnHandle) c.getColumn();
            Domain domain = c.getDomain();

            if (domain.getValues().isNone()) {
                continue;
            }
            if (domain.getValues().isAll()) {
                continue;
            }

            if (domain.isSingleValue()) {
                singleValuesBuilder.put(column, domainValueToString(domain.getSingleValue()));
            }
            else if (isVarcharType(column.getColumnType()) || isCharType(column.getColumnType())) { //the maxcompute sdk can only handle range get partitions on string type
                rangesBuilder.put(column, domain.getValues().getRanges().getOrderedRanges());
            }
        }
        ImmutableMap<MaxComputeColumnHandle, String> singleValues = singleValuesBuilder.build();
        ImmutableMap<MaxComputeColumnHandle, List<Range>> ranges = rangesBuilder.build();

        //make partitionSpc
        PartitionSpec low = new PartitionSpec();
        PartitionSpec high = null;
        for (MaxComputeColumnHandle maxComputeColumnHandle : partitionColumns) {
            String s = singleValues.get(maxComputeColumnHandle);
            if (s == null) {
                high = copyPartitionSpec(low);
                List<Range> range = ranges.get(maxComputeColumnHandle);
                if (range == null || range.isEmpty()) {
                    break;
                }
                Marker lowBound = range.get(0).getLow();
                Marker highBound = range.get(range.size() - 1).getHigh();
                if (lowBound.getValueBlock().isPresent()) {
                    low.set(maxComputeColumnHandle.getColumnName(), domainValueToString(lowBound.getValue()));
                }
                if (highBound.getValueBlock().isPresent()) {
                    high.set(maxComputeColumnHandle.getColumnName(), domainValueToString(highBound.getValue()));
                }
                break;
            }
            low.set(maxComputeColumnHandle.getColumnName(), s);
        }

        if (high == null) {
            high = copyPartitionSpec(low);
        }

        if (!low.isEmpty()) {
            return Optional.of(Pair.of(low, high.isEmpty() ? null : getHigher(high, partitionColumns)));
        }
        else if (!high.isEmpty()) {
            return Optional.of(Pair.of(null, getHigher(high, partitionColumns)));
        }
        else {
            return Optional.empty();
        }
    }

    private static String domainValueToString(Object domainValue)
    {
        if (domainValue instanceof Slice) {
            return ((Slice) domainValue).toStringUtf8();
        }
        else {
            return domainValue.toString();
        }
    }

    private static PartitionSpec copyPartitionSpec(PartitionSpec src)
    {
        if (src == null) {
            return null;
        }
        PartitionSpec dst = new PartitionSpec();
        for (String key : src.keys()) {
            dst.set(key, src.get(key));
        }
        return dst;
    }

    public static PartitionSpec getHigher(PartitionSpec partitionSpec, List<MaxComputeColumnHandle> partitionColumns)
    {
        if (partitionSpec == null) {
            return null;
        }
        PartitionSpec higherPartitionSpc = new PartitionSpec();
        MaxComputeColumnHandle lastColumnHandle = null;
        String lastValue = null;
        for (MaxComputeColumnHandle maxComputeColumnHandle : partitionColumns) {
            String value = partitionSpec.get(maxComputeColumnHandle.getColumnName());
            if (value == null || BOOLEAN.equals(maxComputeColumnHandle.getColumnType())
                    || TIMESTAMP.equals(maxComputeColumnHandle.getColumnType())) {
                break;
            }
            if (lastColumnHandle != null) {
                higherPartitionSpc.set(lastColumnHandle.getColumnName(), lastValue);
            }
            lastColumnHandle = maxComputeColumnHandle;
            lastValue = value;
        }
        if (lastColumnHandle == null) {
            return null;
        }

        String partitionKey = lastColumnHandle.getColumnName();
        higherPartitionSpc.set(partitionKey, lastValue + "1");
        return higherPartitionSpc;
    }

    public static String getPartitionValueOrElse(PartitionSpec partSpec, String partitionKey, String defualtValue)
    {
        String value = partSpec.get(partitionKey);
        return value == null ? defualtValue : value;
    }

    public static boolean matchPartition(PartitionSpec partSpec, TupleDomain<ColumnHandle> tupleDomain)
    {
        Optional<List<ColumnDomain<ColumnHandle>>> cdm = tupleDomain.getColumnDomains();
        if (!cdm.isPresent()) {
            return true;
        }

        Set<String> partKeys = partSpec.keys();
        Map<String, String> nonCaseSensitivePartKV = new HashMap<>();
        for (String k : partKeys) {
            nonCaseSensitivePartKV.put(k.toLowerCase(Locale.getDefault()), partSpec.get(k));
        }

        List<ColumnDomain<ColumnHandle>> cl = cdm.get();
        for (ColumnDomain<ColumnHandle> c : cl) {
            MaxComputeColumnHandle column = (MaxComputeColumnHandle) c.getColumn();
            Domain domain = c.getDomain();
            String columnName = column.getColumnName();

            // Partition spec does not contain the column, avoid it.
            String partKeyValue = nonCaseSensitivePartKV.get(columnName.toLowerCase(Locale.getDefault()));
            if (partKeyValue == null) {
                continue;
            }

            checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

            if (domain.getValues().isNone()) {
                return false;
            }

            if (domain.getValues().isAll()) {
                continue;
            }

            List<Object> singleValues = new ArrayList<>();
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                checkState(!range.isAll()); // Already checked
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
            }

            if (singleValues.size() == 1) {
                // e.g, WHERE pt = 1
                Object v = getOnlyElement(singleValues);
                if (v instanceof Slice) {
                    if (!((Slice) v).toStringUtf8().equals(partKeyValue)) {
                        return false;
                    }
                }
                else {
                    if (!v.toString().equals(partKeyValue)) {
                        return false;
                    }
                }
            }
            else {
                // e.g, WHERE pt = 1 OR pt = 2
                boolean match = false;
                for (Object v : singleValues) {
                    if (v instanceof Slice) {
                        if (((Slice) v).toStringUtf8().equals(partKeyValue)) {
                            match = true;
                            break;
                        }
                    }
                    else {
                        if (v.toString().equals(partKeyValue)) {
                            match = true;
                            break;
                        }
                    }
                }
                if (!match) {
                    return false;
                }
            }
        }

        return true;
    }
}
