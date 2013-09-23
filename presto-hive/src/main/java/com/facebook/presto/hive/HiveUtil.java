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

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.Partition;
import com.google.common.base.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;

final class HiveUtil
{
    // timestamps are stored in local time
    private static final DateTimeFormatter HIVE_TIMESTAMP_PARSER = new DateTimeFormatterBuilder()
            .append(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
            .appendOptional(DateTimeFormat.forPattern(".SSSSSSSSS").getParser())
            .toFormatter();

    private HiveUtil()
    {
    }

    static InputFormat getInputFormat(Configuration configuration, Properties schema, boolean symlinkTarget)
    {
        String inputFormatName = getInputFormatName(schema);
        try {
            JobConf jobConf = new JobConf(configuration);

            // This code should be equivalent to jobConf.getInputFormat()
            Class<? extends InputFormat> inputFormatClass = jobConf.getClassByName(inputFormatName).asSubclass(InputFormat.class);
            if (inputFormatClass == null) {
                // default file format in Hadoop is TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }
            else if (symlinkTarget && (inputFormatClass == SymlinkTextInputFormat.class)) {
                // symlink targets are always TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }
            return ReflectionUtils.newInstance(inputFormatClass, jobConf);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to create input format " + inputFormatName, e);
        }
    }

    static String getInputFormatName(Properties schema)
    {
        String name = schema.getProperty(FILE_INPUT_FORMAT);
        checkArgument(name != null, "missing property: %s", FILE_INPUT_FORMAT);
        return name;
    }

    static ColumnType convertHiveType(String type)
    {
        return HiveType.getSupportedHiveType(convertNativeHiveType(type)).getNativeType();
    }

    static PrimitiveObjectInspector.PrimitiveCategory convertNativeHiveType(String type)
    {
        return PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(type).primitiveCategory;
    }

    public static Function<Partition, String> partitionIdGetter()
    {
        return new Function<Partition, String>()
        {
            @Override
            public String apply(Partition input)
            {
                return input.getPartitionId();
            }
        };
    }

    public static long parseHiveTimestamp(String value)
    {
        return MILLISECONDS.toSeconds(HIVE_TIMESTAMP_PARSER.parseMillis(value));
    }

    public static ObjectInspector getJavaObjectInspector(ObjectInspector objectInspector)
    {
        checkArgument(objectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE, "Unsupported object inspector category %s", objectInspector.getCategory());
        PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) objectInspector);
        switch (poi.getPrimitiveCategory()) {
            case VOID:
                return PrimitiveObjectInspectorFactory.javaVoidObjectInspector;
            case BOOLEAN:
                return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
            case BYTE:
                return PrimitiveObjectInspectorFactory.javaByteObjectInspector;
            case SHORT:
                return PrimitiveObjectInspectorFactory.javaShortObjectInspector;
            case INT:
                return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
            case LONG:
                return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
            case FLOAT:
                return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
            case DOUBLE:
                return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
            case STRING:
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            default:
                throw new RuntimeException("Unknown type: " + poi.getPrimitiveCategory());
        }
    }

    public static DeferredObject getJavaDeferredObject(Object object, ObjectInspector objectInspector)
    {
        checkArgument(objectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE, "Unsupported object inspector category %s", objectInspector.getCategory());
        PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) objectInspector);
        switch (poi.getPrimitiveCategory()) {
            case BOOLEAN: {
                return new DeferredJavaObject((((Long) object).longValue() != 0));
            }
            case BYTE:
                return new DeferredJavaObject(((Long) object).byteValue());
            case SHORT:
                return new DeferredJavaObject(((Long) object).shortValue());
            case INT:
                return new DeferredJavaObject(((Long) object).intValue());
            case LONG:
                return new DeferredJavaObject(object);
            case FLOAT:
                return new DeferredJavaObject(((Double) object).floatValue());
            case DOUBLE:
                return new DeferredJavaObject(object);
            case STRING:
                return new DeferredJavaObject(object);
            default:
                throw new RuntimeException("Unknown type: " + poi.getPrimitiveCategory());
        }
    }

}
