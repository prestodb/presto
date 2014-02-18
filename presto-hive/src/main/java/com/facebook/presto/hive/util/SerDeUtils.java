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
package com.facebook.presto.hive.util;

import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Throwables;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public final class SerDeUtils
{
    private SerDeUtils()
    {
    }

    public static byte[] getJsonBytes(Object object, ObjectInspector objectInspector)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (JsonGenerator generator = new JsonFactory().createGenerator(out);) {
            buildJsonString(generator, object, objectInspector);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return out.toByteArray();
    }

    private static String getPrimitiveAsString(Object object, PrimitiveObjectInspector objectInspector)
    {
        if (object == null) {
            return null;
        }
        switch (objectInspector.getPrimitiveCategory()) {
            case BOOLEAN:
                return Boolean.toString(((BooleanObjectInspector) objectInspector).get(object));
            case BYTE:
                return Byte.toString(((ByteObjectInspector) objectInspector).get(object));
            case SHORT:
                return Short.toString(((ShortObjectInspector) objectInspector).get(object));
            case INT:
                return Integer.toString(((IntObjectInspector) objectInspector).get(object));
            case LONG:
                return Long.toString(((LongObjectInspector) objectInspector).get(object));
            case FLOAT:
                return Float.toString(((FloatObjectInspector) objectInspector).get(object));
            case DOUBLE:
                return Double.toString(((DoubleObjectInspector) objectInspector).get(object));
            case STRING:
                return ((StringObjectInspector) objectInspector).getPrimitiveJavaObject(object);
            case TIMESTAMP:
                return String.valueOf(getTimestampMillis(object, (TimestampObjectInspector) objectInspector));
            case BINARY:
                // Using same Base64 encoder which Jackson uses in JsonGenerator.writeBinary().
                BytesWritable writable = ((BinaryObjectInspector) objectInspector).getPrimitiveWritableObject(object);
                return Base64Variants.getDefaultVariant().encode(Arrays.copyOf(writable.getBytes(), writable.getLength()));
            default:
                throw new RuntimeException("Unknown primitive type: " + objectInspector.getPrimitiveCategory());
        }
    }

    private static void buildJsonString(JsonGenerator generator, Object object, ObjectInspector objectInspector)
            throws IOException
    {
        switch (objectInspector.getCategory()) {
            case PRIMITIVE: {
                PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;
                if (object == null) {
                    generator.writeNull();
                    break;
                }
                switch (primitiveObjectInspector.getPrimitiveCategory()) {
                    case BOOLEAN:
                        generator.writeBoolean(((BooleanObjectInspector) primitiveObjectInspector).get(object));
                        break;
                    case BYTE:
                        generator.writeNumber(((ByteObjectInspector) primitiveObjectInspector).get(object));
                        break;
                    case SHORT:
                        generator.writeNumber(((ShortObjectInspector) primitiveObjectInspector).get(object));
                        break;
                    case INT:
                        generator.writeNumber(((IntObjectInspector) primitiveObjectInspector).get(object));
                        break;
                    case LONG:
                        generator.writeNumber(((LongObjectInspector) primitiveObjectInspector).get(object));
                        break;
                    case FLOAT:
                        generator.writeNumber(((FloatObjectInspector) primitiveObjectInspector).get(object));
                        break;
                    case DOUBLE:
                        generator.writeNumber(((DoubleObjectInspector) primitiveObjectInspector).get(object));
                        break;
                    case STRING:
                        generator.writeString(((StringObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(object));
                        break;
                    case TIMESTAMP:
                        generator.writeNumber(getTimestampMillis(object, (TimestampObjectInspector) objectInspector));
                        break;
                    case BINARY:
                        generator.writeBinary(((BinaryObjectInspector) objectInspector).getPrimitiveJavaObject(object));
                        break;
                    default:
                        throw new RuntimeException("Unknown primitive type: " + primitiveObjectInspector.getPrimitiveCategory());
                }
                break;
            }
            case LIST: {
                ListObjectInspector listInspector = (ListObjectInspector) objectInspector;
                List<?> objectList = listInspector.getList(object);
                if (objectList == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeStartArray();
                    ObjectInspector elementInspector = listInspector.getListElementObjectInspector();
                    for (Object element : objectList) {
                        buildJsonString(generator, element, elementInspector);
                    }
                    generator.writeEndArray();
                }
                break;
            }
            case MAP: {
                MapObjectInspector mapInspector = (MapObjectInspector) objectInspector;
                Map<?, ?> objectMap = mapInspector.getMap(object);
                if (objectMap == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeStartObject();
                    ObjectInspector keyInspector = mapInspector.getMapKeyObjectInspector();
                    checkState(keyInspector instanceof PrimitiveObjectInspector);
                    ObjectInspector valueInspector = mapInspector.getMapValueObjectInspector();
                    for (Map.Entry<?, ?> entry : objectMap.entrySet()) {
                        generator.writeFieldName(getPrimitiveAsString(entry.getKey(), (PrimitiveObjectInspector) keyInspector));
                        buildJsonString(generator, entry.getValue(), valueInspector);
                    }
                    generator.writeEndObject();
                }
                break;
            }
            case STRUCT: {
                if (object == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeStartObject();
                    StructObjectInspector structInspector = (StructObjectInspector) objectInspector;
                    List<? extends StructField> structFields = structInspector.getAllStructFieldRefs();
                    for (int i = 0; i < structFields.size(); i++) {
                        generator.writeFieldName(structFields.get(i).getFieldName());
                        buildJsonString(generator, structInspector.getStructFieldData(object, structFields.get(i)), structFields.get(i).getFieldObjectInspector());
                    }
                    generator.writeEndObject();
                }
                break;
            }
            case UNION: {
                if (object == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeStartObject();
                    UnionObjectInspector unionInspector = (UnionObjectInspector) objectInspector;
                    generator.writeFieldName(Byte.toString(unionInspector.getTag(object)));
                    buildJsonString(generator, unionInspector.getField(object), unionInspector.getObjectInspectors().get(unionInspector.getTag(object)));
                    generator.writeEndObject();
                }
                break;
            }
            default:
                throw new RuntimeException("Unknown type in ObjectInspector!" + objectInspector.getCategory());
        }
    }

    private static long getTimestampMillis(Object object, TimestampObjectInspector objectInspector)
    {
        TimestampWritable timestampWritable = objectInspector.getPrimitiveWritableObject(object);
        long seconds = timestampWritable.getSeconds();
        long nanos = timestampWritable.getNanos();
        return (seconds * 1000) + (nanos / 1_000_000);
    }
}
