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

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.hive.HiveUtil.getTableStructFields;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Sets.immutableEnumSet;
import static java.util.Map.Entry;
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

    public static Optional<HiveBucket> getHiveBucket(Table table, Map<ConnectorColumnHandle, ?> bindings)
    {
        if (!table.getSd().isSetBucketCols() || table.getSd().getBucketCols().isEmpty() ||
                !table.getSd().isSetNumBuckets() || (table.getSd().getNumBuckets() <= 0) ||
                bindings.isEmpty()) {
            return Optional.absent();
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
                return Optional.absent();
            }
            if (!SUPPORTED_TYPES.contains(((PrimitiveObjectInspector) inspector).getPrimitiveCategory())) {
                return Optional.absent();
            }
        }

        // Get bindings for bucket columns
        Map<String, Object> bucketBindings = new HashMap<>();
        for (Entry<ConnectorColumnHandle, ?> entry : bindings.entrySet()) {
            HiveColumnHandle colHandle = (HiveColumnHandle) entry.getKey();
            if (bucketColumns.contains(colHandle.getName())) {
                bucketBindings.put(colHandle.getName(), entry.getValue());
            }
        }

        // Check that we have bindings for all bucket columns
        if (bucketBindings.size() != bucketColumns.size()) {
            return Optional.absent();
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
            return Optional.absent();
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
            return Objects.toStringHelper(this)
                    .add("bucketNumber", bucketNumber)
                    .add("bucketCount", bucketCount)
                    .toString();
        }
    }
}
