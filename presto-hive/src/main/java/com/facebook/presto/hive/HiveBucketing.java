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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveUtil.getRegularColumnHandles;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

final class HiveBucketing
{
    private static final Set<HiveType> SUPPORTED_TYPES_FOR_BUCKET_FILTER = ImmutableSet.of(
            HiveType.HIVE_BYTE,
            HiveType.HIVE_SHORT,
            HiveType.HIVE_INT,
            HiveType.HIVE_LONG,
            HiveType.HIVE_BOOLEAN,
            HiveType.HIVE_STRING);

    private HiveBucketing() {}

    public static int getVirtualBucketNumber(int bucketCount, Path path)
    {
        // this is equivalent to bucketing the table on a VARCHAR column containing $path
        return (hashBytes(0, utf8Slice(path.toString())) & Integer.MAX_VALUE) % bucketCount;
    }

    public static int getHiveBucket(int bucketCount, List<TypeInfo> types, Page page, int position)
    {
        return (getBucketHashCode(types, page, position) & Integer.MAX_VALUE) % bucketCount;
    }

    public static int getHiveBucket(int bucketCount, List<TypeInfo> types, Object[] values)
    {
        return (getBucketHashCode(types, values) & Integer.MAX_VALUE) % bucketCount;
    }

    private static int getBucketHashCode(List<TypeInfo> types, Page page, int position)
    {
        checkArgument(types.size() == page.getChannelCount());
        int result = 0;
        for (int i = 0; i < page.getChannelCount(); i++) {
            int fieldHash = hash(types.get(i), page.getBlock(i), position);
            result = result * 31 + fieldHash;
        }
        return result;
    }

    private static int getBucketHashCode(List<TypeInfo> types, Object[] values)
    {
        checkArgument(types.size() == values.length);
        int result = 0;
        for (int i = 0; i < values.length; i++) {
            int fieldHash = hash(types.get(i), values[i]);
            result = result * 31 + fieldHash;
        }
        return result;
    }

    private static int hash(TypeInfo type, Block block, int position)
    {
        // This function mirrors the behavior of function hashCode in
        // HIVE-12025 ba83fd7bff serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java
        // https://github.com/apache/hive/blob/ba83fd7bff/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java

        // HIVE-7148 proposed change to bucketing hash algorithms. If that gets implemented, this function will need to change significantly.

        if (block.isNull(position)) {
            return 0;
        }

        switch (type.getCategory()) {
            case PRIMITIVE: {
                PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) type;
                PrimitiveCategory primitiveCategory = typeInfo.getPrimitiveCategory();
                Type prestoType = requireNonNull(HiveType.getPrimitiveType(typeInfo));
                switch (primitiveCategory) {
                    case BOOLEAN:
                        return prestoType.getBoolean(block, position) ? 1 : 0;
                    case BYTE:
                        return SignedBytes.checkedCast(prestoType.getLong(block, position));
                    case SHORT:
                        return Shorts.checkedCast(prestoType.getLong(block, position));
                    case INT:
                        return toIntExact(prestoType.getLong(block, position));
                    case LONG:
                        long bigintValue = prestoType.getLong(block, position);
                        return (int) ((bigintValue >>> 32) ^ bigintValue);
                    case FLOAT:
                        return (int) prestoType.getLong(block, position);
                    case DOUBLE:
                        long doubleValue = doubleToLongBits(prestoType.getDouble(block, position));
                        return (int) ((doubleValue >>> 32) ^ doubleValue);
                    case STRING:
                        return hashBytes(0, prestoType.getSlice(block, position));
                    case VARCHAR:
                        return hashBytes(1, prestoType.getSlice(block, position));
                    case DATE:
                        // day offset from 1970-01-01
                        long days = prestoType.getLong(block, position);
                        return toIntExact(days);
                    case TIMESTAMP:
                        long millisSinceEpoch = prestoType.getLong(block, position);
                        // seconds << 30 + nanoseconds
                        long secondsAndNanos = (Math.floorDiv(millisSinceEpoch, 1000L) << 30) + Math.floorMod(millisSinceEpoch, 1000);
                        return (int) ((secondsAndNanos >>> 32) ^ secondsAndNanos);
                    default:
                        throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive primitive category: " + primitiveCategory.toString() + ".");
                }
            }
            case LIST: {
                Block elementsBlock = block.getObject(position, Block.class);
                return hashOfList((ListTypeInfo) type, elementsBlock);
            }
            case MAP: {
                Block elementsBlock = block.getObject(position, Block.class);
                return hashOfMap((MapTypeInfo) type, elementsBlock);
            }
            default:
                // TODO: support more types, e.g. ROW
                throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive category: " + type.getCategory().toString() + ".");
        }
    }

    private static int hash(TypeInfo type, Object value)
    {
        if (value == null) {
            return 0;
        }

        switch (type.getCategory()) {
            case PRIMITIVE: {
                PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) type;
                PrimitiveCategory primitiveCategory = typeInfo.getPrimitiveCategory();
                Type prestoType = requireNonNull(HiveType.getPrimitiveType(typeInfo));
                switch (primitiveCategory) {
                    case BOOLEAN:
                        return (boolean) value ? 1 : 0;
                    case BYTE:
                        return SignedBytes.checkedCast((long) value);
                    case SHORT:
                        return Shorts.checkedCast((long) value);
                    case INT:
                        return toIntExact((long) value);
                    case LONG:
                        long bigintValue = (long) value;
                        return (int) ((bigintValue >>> 32) ^ bigintValue);
                    case FLOAT:
                        return (int) (long) value;
                    case DOUBLE:
                        long doubleValue = doubleToLongBits((double) value);
                        return (int) ((doubleValue >>> 32) ^ doubleValue);
                    case STRING:
                        return hashBytes(0, (Slice) value);
                    case VARCHAR:
                        return hashBytes(1, (Slice) value);
                    case DATE:
                        // day offset from 1970-01-01
                        long days = (long) value;
                        return toIntExact(days);
                    case TIMESTAMP:
                        long millisSinceEpoch = (long) value;
                        // seconds << 30 + nanoseconds
                        long secondsAndNanos = (Math.floorDiv(millisSinceEpoch, 1000L) << 30) + Math.floorMod(millisSinceEpoch, 1000);
                        return (int) ((secondsAndNanos >>> 32) ^ secondsAndNanos);
                    default:
                        throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive primitive category: " + primitiveCategory.toString() + ".");
                }
            }
            case LIST: {
                return hashOfList((ListTypeInfo) type, (Block) value);
            }
            case MAP: {
                return hashOfMap((MapTypeInfo) type, (Block) value);
            }
            default:
                // TODO: support more types, e.g. ROW
                throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive category: " + type.getCategory().toString() + ".");
        }
    }

    private static int hashOfMap(MapTypeInfo type, Block singleMapBlock)
    {
        TypeInfo keyTypeInfo = type.getMapKeyTypeInfo();
        TypeInfo valueTypeInfo = type.getMapValueTypeInfo();
        int result = 0;
        for (int i = 0; i < singleMapBlock.getPositionCount(); i += 2) {
            result += hash(keyTypeInfo, singleMapBlock, i) ^ hash(valueTypeInfo, singleMapBlock, i + 1);
        }
        return result;
    }

    private static int hashOfList(ListTypeInfo type, Block singleListBlock)
    {
        TypeInfo elementTypeInfo = type.getListElementTypeInfo();
        int result = 0;
        for (int i = 0; i < singleListBlock.getPositionCount(); i++) {
            result = result * 31 + hash(elementTypeInfo, singleListBlock, i);
        }
        return result;
    }

    private static int hashBytes(int initialValue, Slice bytes)
    {
        int result = initialValue;
        for (int i = 0; i < bytes.length(); i++) {
            result = result * 31 + bytes.getByte(i);
        }
        return result;
    }

    public static Optional<HiveBucketHandle> getHiveBucketHandle(Table table)
    {
        Optional<HiveBucketProperty> hiveBucketProperty = table.getStorage().getBucketProperty();
        if (!hiveBucketProperty.isPresent()) {
            return Optional.empty();
        }

        Map<String, HiveColumnHandle> map = getRegularColumnHandles(table).stream()
                .collect(Collectors.toMap(HiveColumnHandle::getName, identity()));

        ImmutableList.Builder<HiveColumnHandle> bucketColumns = ImmutableList.builder();
        for (String bucketColumnName : hiveBucketProperty.get().getBucketedBy()) {
            HiveColumnHandle bucketColumnHandle = map.get(bucketColumnName);
            if (bucketColumnHandle == null) {
                throw new PrestoException(
                        HIVE_INVALID_METADATA,
                        format("Table '%s.%s' is bucketed on non-existent column '%s'", table.getDatabaseName(), table.getTableName(), bucketColumnName));
            }
            bucketColumns.add(bucketColumnHandle);
        }

        int bucketCount = hiveBucketProperty.get().getBucketCount();
        return Optional.of(new HiveBucketHandle(bucketColumns.build(), bucketCount, bucketCount));
    }

    public static Optional<HiveBucketFilter> getHiveBucketFilter(Table table, TupleDomain<ColumnHandle> effectivePredicate)
    {
        if (!table.getStorage().getBucketProperty().isPresent()) {
            return Optional.empty();
        }

        Optional<Map<ColumnHandle, NullableValue>> bindings = TupleDomain.extractFixedValues(effectivePredicate);
        if (!bindings.isPresent()) {
            return Optional.empty();
        }
        OptionalInt singleBucket = getHiveBucket(table, bindings.get());
        if (singleBucket.isPresent()) {
            return Optional.of(new HiveBucketFilter(ImmutableSet.of(singleBucket.getAsInt())));
        }

        if (!effectivePredicate.getDomains().isPresent()) {
            return Optional.empty();
        }
        Optional<Domain> domain = effectivePredicate.getDomains().get().entrySet().stream()
                .filter(entry -> ((HiveColumnHandle) entry.getKey()).getName().equals(BUCKET_COLUMN_NAME))
                .findFirst()
                .map(Entry::getValue);
        if (!domain.isPresent()) {
            return Optional.empty();
        }
        ValueSet values = domain.get().getValues();
        ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        int bucketCount = table.getStorage().getBucketProperty().get().getBucketCount();
        for (int i = 0; i < bucketCount; i++) {
            if (values.containsValue((long) i)) {
                builder.add(i);
            }
        }
        return Optional.of(new HiveBucketFilter(builder.build()));
    }

    private static OptionalInt getHiveBucket(Table table, Map<ColumnHandle, NullableValue> bindings)
    {
        if (bindings.isEmpty()) {
            return OptionalInt.empty();
        }

        List<String> bucketColumns = table.getStorage().getBucketProperty().get().getBucketedBy();
        Map<String, HiveType> hiveTypes = new HashMap<>();
        for (Column column : table.getDataColumns()) {
            hiveTypes.put(column.getName(), column.getType());
        }

        // Verify the bucket column types are supported
        for (String column : bucketColumns) {
            if (!SUPPORTED_TYPES_FOR_BUCKET_FILTER.contains(hiveTypes.get(column))) {
                return OptionalInt.empty();
            }
        }

        // Get bindings for bucket columns
        Map<String, Object> bucketBindings = new HashMap<>();
        for (Entry<ColumnHandle, NullableValue> entry : bindings.entrySet()) {
            HiveColumnHandle colHandle = (HiveColumnHandle) entry.getKey();
            if (!entry.getValue().isNull() && bucketColumns.contains(colHandle.getName())) {
                bucketBindings.put(colHandle.getName(), entry.getValue().getValue());
            }
        }

        // Check that we have bindings for all bucket columns
        if (bucketBindings.size() != bucketColumns.size()) {
            return OptionalInt.empty();
        }

        // Get bindings of bucket columns
        ImmutableList.Builder<TypeInfo> typeInfos = ImmutableList.builder();
        Object[] values = new Object[bucketColumns.size()];
        for (int i = 0; i < bucketColumns.size(); i++) {
            String column = bucketColumns.get(i);
            typeInfos.add(hiveTypes.get(column).getTypeInfo());
            values[i] = bucketBindings.get(column);
        }

        return OptionalInt.of(getHiveBucket(table.getStorage().getBucketProperty().get().getBucketCount(), typeInfos.build(), values));
    }

    public static class HiveBucketFilter
    {
        private final Set<Integer> bucketsToKeep;

        @JsonCreator
        public HiveBucketFilter(@JsonProperty("bucketsToKeep") Set<Integer> bucketsToKeep)
        {
            this.bucketsToKeep = bucketsToKeep;
        }

        @JsonProperty
        public Set<Integer> getBucketsToKeep()
        {
            return bucketsToKeep;
        }
    }
}
