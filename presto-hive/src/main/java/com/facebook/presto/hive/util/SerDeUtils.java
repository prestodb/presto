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
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.util.Types.checkType;

public final class SerDeUtils
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private SerDeUtils() {}

    public static byte[] getJsonBytes(DateTimeZone sessionTimeZone, Object object, ObjectInspector objectInspector)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (JsonGenerator generator = new JsonFactory().createGenerator(out)) {
            serializeObject(sessionTimeZone, generator, object, objectInspector, null);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return out.toByteArray();
    }

    private static void serializeObject(DateTimeZone sessionTimeZone, JsonGenerator generator, Object object, ObjectInspector inspector, JsonContext context)
            throws IOException
    {
        switch (inspector.getCategory()) {
            case PRIMITIVE:
                serializePrimitive(sessionTimeZone, generator, object, (PrimitiveObjectInspector) inspector, context);
                return;
            case LIST:
                serializeList(sessionTimeZone, generator, object, (ListObjectInspector) inspector, context);
                return;
            case MAP:
                serializeMap(sessionTimeZone, generator, object, (MapObjectInspector) inspector, context);
                return;
            case STRUCT:
                serializeStruct(sessionTimeZone, generator, object, (StructObjectInspector) inspector, context);
                return;
        }
        throw new RuntimeException("Unknown object inspector category: " + inspector.getCategory());
    }

    private static void serializePrimitive(DateTimeZone sessionTimeZone, JsonGenerator generator, Object object, PrimitiveObjectInspector inspector, JsonContext context)
            throws IOException
    {
        if (object == null) {
            generator.writeNull();
            return;
        }

        switch (inspector.getPrimitiveCategory()) {
            case BOOLEAN:
                generator.writeBoolean(((BooleanObjectInspector) inspector).get(object));
                return;
            case BYTE:
                generator.writeNumber(((ByteObjectInspector) inspector).get(object));
                return;
            case SHORT:
                generator.writeNumber(((ShortObjectInspector) inspector).get(object));
                return;
            case INT:
                generator.writeNumber(((IntObjectInspector) inspector).get(object));
                return;
            case LONG:
                generator.writeNumber(((LongObjectInspector) inspector).get(object));
                return;
            case FLOAT:
                generator.writeNumber(((FloatObjectInspector) inspector).get(object));
                return;
            case DOUBLE:
                generator.writeNumber(((DoubleObjectInspector) inspector).get(object));
                return;
            case STRING:
                generator.writeString(((StringObjectInspector) inspector).getPrimitiveJavaObject(object));
                return;
            case DATE:
                if (context == JsonContext.JSON_STACK) {
                    generator.writeNumber(formatDateAsLong(object, (DateObjectInspector) inspector));
                }
                else {
                    generator.writeString(formatDate(object, (DateObjectInspector) inspector));
                }
                return;
            case TIMESTAMP:
                if (context == JsonContext.JSON_STACK) {
                    generator.writeNumber(formatTimestampAsLong(object, (TimestampObjectInspector) inspector));
                }
                else {
                    generator.writeString(formatTimestamp(sessionTimeZone, object, (TimestampObjectInspector) inspector));
                }
                return;
            case BINARY:
                generator.writeBinary(((BinaryObjectInspector) inspector).getPrimitiveJavaObject(object));
                return;
        }
        throw new RuntimeException("Unknown primitive type: " + inspector.getPrimitiveCategory());
    }

    private static void serializeList(DateTimeZone sessionTimeZone, JsonGenerator generator, Object object, ListObjectInspector inspector, JsonContext context)
            throws IOException
    {
        List<?> list = inspector.getList(object);
        if (list == null) {
            generator.writeNull();
            return;
        }

        ObjectInspector elementInspector = inspector.getListElementObjectInspector();

        generator.writeStartArray();
        for (Object element : list) {
            serializeObject(sessionTimeZone, generator, element, elementInspector, context == null ? JsonContext.JSON_STACK : context);
        }
        generator.writeEndArray();
    }

    private static void serializeMap(DateTimeZone sessionTimeZone, JsonGenerator generator, Object object, MapObjectInspector inspector, JsonContext context)
            throws IOException
    {
        Map<?, ?> map = inspector.getMap(object);
        if (map == null) {
            generator.writeNull();
            return;
        }

        PrimitiveObjectInspector keyInspector = checkType(inspector.getMapKeyObjectInspector(), PrimitiveObjectInspector.class, "map key inspector");
        ObjectInspector valueInspector = inspector.getMapValueObjectInspector();

        generator.writeStartObject();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Hive skips map entries with null keys
            if (entry.getKey() != null) {
                generator.writeFieldName(getPrimitiveAsString(sessionTimeZone, entry.getKey(), keyInspector, context == null ? JsonContext.JSON_STACK : context));
                serializeObject(sessionTimeZone, generator, entry.getValue(), valueInspector, context == null ? JsonContext.JSON_STACK : context);
            }
        }
        generator.writeEndObject();
    }

    private static void serializeStruct(DateTimeZone sessionTimeZone, JsonGenerator generator, Object object, StructObjectInspector inspector, JsonContext context)
            throws IOException
    {
        if (object == null) {
            generator.writeNull();
            return;
        }

        generator.writeStartArray();
        for (StructField field : inspector.getAllStructFieldRefs()) {
            serializeObject(sessionTimeZone, generator, inspector.getStructFieldData(object, field), field.getFieldObjectInspector(), JsonContext.JSON_STACK);
        }
        generator.writeEndArray();
    }

    private static String getPrimitiveAsString(DateTimeZone sessionTimeZone, Object object, PrimitiveObjectInspector inspector, JsonContext context)
    {
        switch (inspector.getPrimitiveCategory()) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
                return String.valueOf(inspector.getPrimitiveJavaObject(object));
            case DATE:
                if (context == JsonContext.JSON_STACK) {
                    return String.valueOf(formatDateAsLong(object, (DateObjectInspector) inspector));
                }
                else {
                    return formatDate(object, (DateObjectInspector) inspector);
                }
            case TIMESTAMP:
                if (context == JsonContext.JSON_STACK) {
                    return String.valueOf(formatTimestampAsLong(object, (TimestampObjectInspector) inspector));
                }
                else {
                    return formatTimestamp(sessionTimeZone, object, (TimestampObjectInspector) inspector);
                }
            case BINARY:
                // Using same Base64 encoder which Jackson uses in JsonGenerator.writeBinary().
                BytesWritable writable = ((BinaryObjectInspector) inspector).getPrimitiveWritableObject(object);
                return Base64Variants.getDefaultVariant().encode(Arrays.copyOf(writable.getBytes(), writable.getLength()));
            default:
                throw new RuntimeException("Unknown primitive type: " + inspector.getPrimitiveCategory());
        }
    }

    private static String formatDate(Object object, DateObjectInspector inspector)
    {
        if (object instanceof LazyDate) {
            int days = ((LazyDate) object).getWritableObject().getDays();
            // Render in UTC because we are giving the date formatter milliseconds since 1970-01-01 00:00 UTC
            return DATE_FORMATTER.withZoneUTC().print(days * MILLIS_IN_DAY);
        }
        if (object instanceof DateWritable) {
            int days = ((DateWritable) object).getDays();
            // Render in UTC because we are giving the date formatter milliseconds since 1970-01-01 00:00 UTC
            return DATE_FORMATTER.withZoneUTC().print(days * MILLIS_IN_DAY);
        }

        // convert date from VM current time zone to UTC
        Date date = inspector.getPrimitiveJavaObject(object);
        long storageTime = date.getTime();
        long utcMillis = storageTime + DateTimeZone.getDefault().getOffset(storageTime);
        return DATE_FORMATTER.withZone(DateTimeZone.UTC).print(utcMillis);
    }

    private static long formatDateAsLong(Object object, DateObjectInspector inspector)
    {
        if (object instanceof LazyDate) {
            return ((LazyDate) object).getWritableObject().getDays();
        }
        if (object instanceof DateWritable) {
            return ((DateWritable) object).getDays();
        }

        // Hive will return java.sql.Date at midnight in JVM time zone
        long millisLocal = inspector.getPrimitiveJavaObject(object).getTime();
        // Convert it to midnight in UTC
        long millisUtc = DateTimeZone.getDefault().getMillisKeepLocal(DateTimeZone.UTC, millisLocal);
        // Convert midnight UTC to days
        return TimeUnit.MILLISECONDS.toDays(millisUtc);
    }

    private static long formatTimestampAsLong(Object object, TimestampObjectInspector inspector)
    {
        Timestamp timestamp = getTimestamp(object, inspector);
        return timestamp.getTime();
    }

    private static String formatTimestamp(DateTimeZone sessionTimeZone, Object object, TimestampObjectInspector inspector)
    {
        Timestamp timestamp = getTimestamp(object, inspector);
        return TIMESTAMP_FORMATTER.withZone(sessionTimeZone).print(timestamp.getTime());
    }

    private static Timestamp getTimestamp(Object object, TimestampObjectInspector inspector)
    {
        // handle broken ObjectInspectors
        if (object instanceof TimestampWritable) {
            return ((TimestampWritable) object).getTimestamp();
        }
        return inspector.getPrimitiveJavaObject(object);
    }

    public enum JsonContext
    {
        JSON_STACK,
        JSON
    }
}
