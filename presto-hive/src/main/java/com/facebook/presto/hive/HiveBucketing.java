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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.NullableValue;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.DefaultHivePartitioner;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveUtil.getNonPartitionKeyColumnHandles;
import static com.facebook.presto.hive.HiveUtil.getTableStructFields;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Sets.immutableEnumSet;
import static java.lang.Double.doubleToLongBits;
import static java.util.Map.Entry;
import static java.util.function.Function.identity;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

final class HiveBucketing
{
    private static final Logger log = Logger.get(HiveBucketing.class);

    private static final Set<PrimitiveCategory> SUPPORTED_TYPES = immutableEnumSet(
            PrimitiveCategory.BYTE,
            PrimitiveCategory.SHORT,
            PrimitiveCategory.INT,
            PrimitiveCategory.LONG,
            PrimitiveCategory.BOOLEAN,
            PrimitiveCategory.STRING);

    private HiveBucketing() {}

    public static int getHiveBucket(List<TypeInfo> types, Page page, int position, int bucketCount)
    {
        return (getBucketHashCode(types, page, position) & Integer.MAX_VALUE) % bucketCount;
    }

    private static int getBucketHashCode(List<TypeInfo> types, Page page, int position)
    {
        int result = 0;
        for (int i = 0; i < page.getChannelCount(); i++) {
            int fieldHash = hash(types.get(i), page.getBlock(i), position);
            result = result * 31 + fieldHash;
        }
        return result;
    }

    private static int hash(TypeInfo type, Block block, int position)
    {
        // This function mirrors the behavior of function hashCode in
        // HIVE-12025 ba83fd7bff serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java

        // HIVE-7148 proposed change to bucketing hash algorithms. If that gets implemented, this function will need to change significantly.

        if (block.isNull(position)) {
            return 0;
        }

        switch (type.getCategory()) {
            case PRIMITIVE:
                PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) type).getPrimitiveCategory();
                switch (primitiveCategory) {
                    case BOOLEAN:
                        return BOOLEAN.getBoolean(block, position) ? 1 : 0;
                    case BYTE:
                        return SignedBytes.checkedCast(INTEGER.getLong(block, position));
                    case SHORT:
                        return Shorts.checkedCast(INTEGER.getLong(block, position));
                    case INT:
                        return Ints.checkedCast(INTEGER.getLong(block, position));
                    case LONG:
                        long bigintValue = BIGINT.getLong(block, position);
                        return (int) ((bigintValue >>> 32) ^ bigintValue);
                    case FLOAT:
                        return Float.floatToIntBits((float) DOUBLE.getDouble(block, position));
                    case DOUBLE:
                        long doubleValue = doubleToLongBits(DOUBLE.getDouble(block, position));
                        return (int) ((doubleValue >>> 32) ^ doubleValue);
                    case STRING:
                        return hashBytes(0, createUnboundedVarcharType().getSlice(block, position));
                    case DATE:
                        // day offset from 1970-01-01
                        long days = DATE.getLong(block, position);
                        return Ints.checkedCast(days);
                    case TIMESTAMP:
                        long millisSinceEpoch = TIMESTAMP.getLong(block, position);
                        // seconds << 30 + nanoseconds
                        long secondsAndNanos = (Math.floorDiv(millisSinceEpoch, 1000L) << 30) + Math.floorMod(millisSinceEpoch, 1000);
                        return (int) ((secondsAndNanos >>> 32) ^ secondsAndNanos);
                    default:
                        throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive primitive category: " + primitiveCategory.toString() + ".");
                }
            case LIST: {
                TypeInfo elementTypeInfo = ((ListTypeInfo) type).getListElementTypeInfo();
                Block elementsBlock = block.getObject(position, Block.class);
                int result = 0;
                for (int i = 0; i < elementsBlock.getPositionCount(); i++) {
                    result = result * 31 + hash(elementTypeInfo, elementsBlock, i);
                }
                return result;
            }
            case MAP: {
                MapTypeInfo mapTypeInfo = (MapTypeInfo) type;
                TypeInfo keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
                TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();
                Block elementsBlock = block.getObject(position, Block.class);
                int result = 0;
                for (int i = 0; i < elementsBlock.getPositionCount(); i += 2) {
                    result += hash(keyTypeInfo, elementsBlock, i) ^ hash(valueTypeInfo, elementsBlock, i + 1);
                }
                return result;
            }
            default:
                // TODO: support more types, e.g. ROW
                throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive category: " + type.getCategory().toString() + ".");
        }
    }

    private static int hashBytes(int initialValue, Slice bytes)
    {
        int result = initialValue;
        for (int i = 0; i < bytes.length(); i++) {
            result = result * 31 + bytes.getByte(i);
        }
        return result;
    }

    public static Optional<HiveBucketHandle> getHiveBucketHandle(String connectorId, Table table)
    {
        Optional<HiveBucketProperty> hiveBucketProperty = HiveBucketProperty.fromStorageDescriptor(table.getSd(), table.getTableName());
        if (!hiveBucketProperty.isPresent()) {
            return Optional.empty();
        }

        Map<String, HiveColumnHandle> map = getNonPartitionKeyColumnHandles(connectorId, table).stream()
                .collect(Collectors.toMap(HiveColumnHandle::getName, identity()));

        List<HiveColumnHandle> bucketColumns = hiveBucketProperty.get().getBucketedBy().stream()
                .map(map::get)
                .collect(Collectors.toList());

        return Optional.of(new HiveBucketHandle(bucketColumns, hiveBucketProperty.get().getBucketCount()));
    }

    public static Optional<HiveBucket> getHiveBucket(Table table, Map<ColumnHandle, NullableValue> bindings)
    {
        if (!table.getSd().isSetBucketCols() || table.getSd().getBucketCols().isEmpty() ||
                !table.getSd().isSetNumBuckets() || (table.getSd().getNumBuckets() <= 0) ||
                bindings.isEmpty()) {
            return Optional.empty();
        }

        List<String> bucketColumns = table.getSd().getBucketCols();
        Map<String, ObjectInspector> objectInspectors = new HashMap<>();

        // Get column name to object inspector mapping
        for (StructField field : getTableStructFields(table)) {
            objectInspectors.put(field.getFieldName(), field.getFieldObjectInspector());
        }

        // Verify the bucket column types are supported
        for (String column : bucketColumns) {
            ObjectInspector inspector = objectInspectors.get(column);
            if ((inspector == null) || (inspector.getCategory() != Category.PRIMITIVE)) {
                return Optional.empty();
            }
            if (!SUPPORTED_TYPES.contains(((PrimitiveObjectInspector) inspector).getPrimitiveCategory())) {
                return Optional.empty();
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
            return Optional.empty();
        }

        // Get bindings of bucket columns
        ImmutableList.Builder<Entry<ObjectInspector, Object>> columnBindings = ImmutableList.builder();
        for (String column : bucketColumns) {
            columnBindings.add(immutableEntry(objectInspectors.get(column), bucketBindings.get(column)));
        }

        return getHiveBucket(columnBindings.build(), table.getSd().getNumBuckets());
    }

    public static Optional<HiveBucket> getHiveBucket(List<Entry<ObjectInspector, Object>> columnBindings, int bucketCount)
    {
        try {
            @SuppressWarnings("resource")
            GenericUDFHash udf = new GenericUDFHash();
            ObjectInspector[] objectInspectors = new ObjectInspector[columnBindings.size()];
            DeferredObject[] deferredObjects = new DeferredObject[columnBindings.size()];

            int i = 0;
            for (Entry<ObjectInspector, Object> entry : columnBindings) {
                objectInspectors[i] = getJavaObjectInspector(entry.getKey());
                deferredObjects[i] = getJavaDeferredObject(entry.getValue(), entry.getKey());
                i++;
            }

            ObjectInspector udfInspector = udf.initialize(objectInspectors);
            IntObjectInspector inspector = checkType(udfInspector, IntObjectInspector.class, "udfInspector");

            Object result = udf.evaluate(deferredObjects);
            HiveKey hiveKey = new HiveKey();
            hiveKey.setHashCode(inspector.get(result));

            int bucketNumber = new DefaultHivePartitioner<>().getBucket(hiveKey, null, bucketCount);

            return Optional.of(new HiveBucket(bucketNumber, bucketCount));
        }
        catch (HiveException e) {
            log.debug(e, "Error evaluating bucket number");
            return Optional.empty();
        }
    }

    private static ObjectInspector getJavaObjectInspector(ObjectInspector objectInspector)
    {
        checkArgument(objectInspector.getCategory() == Category.PRIMITIVE, "Unsupported object inspector category %s", objectInspector.getCategory());
        PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) objectInspector);
        switch (poi.getPrimitiveCategory()) {
            case BOOLEAN:
                return javaBooleanObjectInspector;
            case BYTE:
                return javaByteObjectInspector;
            case SHORT:
                return javaShortObjectInspector;
            case INT:
                return javaIntObjectInspector;
            case LONG:
                return javaLongObjectInspector;
            case STRING:
                return javaStringObjectInspector;
        }
        throw new RuntimeException("Unsupported type: " + poi.getPrimitiveCategory());
    }

    private static DeferredObject getJavaDeferredObject(Object object, ObjectInspector objectInspector)
    {
        checkArgument(objectInspector.getCategory() == Category.PRIMITIVE, "Unsupported object inspector category %s", objectInspector.getCategory());
        PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) objectInspector);
        switch (poi.getPrimitiveCategory()) {
            case BOOLEAN:
                return new DeferredJavaObject(object);
            case BYTE:
                return new DeferredJavaObject(((Long) object).byteValue());
            case SHORT:
                return new DeferredJavaObject(((Long) object).shortValue());
            case INT:
                return new DeferredJavaObject(((Long) object).intValue());
            case LONG:
                return new DeferredJavaObject(object);
            case STRING:
                return new DeferredJavaObject(((Slice) object).toStringUtf8());
        }
        throw new RuntimeException("Unsupported type: " + poi.getPrimitiveCategory());
    }

    public static class HiveBucket
    {
        private final int bucketNumber;
        private final int bucketCount;

        public HiveBucket(int bucketNumber, int bucketCount)
        {
            checkArgument(bucketCount > 0, "bucketCount must be greater than zero");
            checkArgument(bucketNumber >= 0, "bucketCount must be positive");
            checkArgument(bucketNumber < bucketCount, "bucketNumber must be less than bucketCount");

            this.bucketNumber = bucketNumber;
            this.bucketCount = bucketCount;
        }

        public int getBucketNumber()
        {
            return bucketNumber;
        }

        public int getBucketCount()
        {
            return bucketCount;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("bucketNumber", bucketNumber)
                    .add("bucketCount", bucketCount)
                    .toString();
        }
    }
}
