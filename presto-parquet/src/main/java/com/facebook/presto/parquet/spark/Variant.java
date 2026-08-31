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
package com.facebook.presto.parquet.spark;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Base64;
import java.util.Locale;

import static com.facebook.presto.parquet.spark.VariantUtil.SIZE_LIMIT;
import static com.facebook.presto.parquet.spark.VariantUtil.VERSION;
import static com.facebook.presto.parquet.spark.VariantUtil.VERSION_MASK;
import static com.facebook.presto.parquet.spark.VariantUtil.getBinary;
import static com.facebook.presto.parquet.spark.VariantUtil.getBoolean;
import static com.facebook.presto.parquet.spark.VariantUtil.getDecimal;
import static com.facebook.presto.parquet.spark.VariantUtil.getDouble;
import static com.facebook.presto.parquet.spark.VariantUtil.getFloat;
import static com.facebook.presto.parquet.spark.VariantUtil.getLong;
import static com.facebook.presto.parquet.spark.VariantUtil.getMetadataKey;
import static com.facebook.presto.parquet.spark.VariantUtil.getString;
import static com.facebook.presto.parquet.spark.VariantUtil.getType;
import static com.facebook.presto.parquet.spark.VariantUtil.handleArray;
import static com.facebook.presto.parquet.spark.VariantUtil.handleObject;
import static com.facebook.presto.parquet.spark.VariantUtil.readUnsigned;
import static com.google.common.base.Preconditions.checkArgument;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoUnit.MICROS;

/**
 * Copied from https://github.com/apache/spark/blob/53d65fd12dd9231139188227ef9040d40d759021/common/variant/src/main/java/org/apache/spark/types/variant/Variant.java
 * and adjusted the code style.
 */
public final class Variant
{
    private static final DateTimeFormatter TIMESTAMP_NTZ_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME)
            .toFormatter(Locale.US);

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .append(TIMESTAMP_NTZ_FORMATTER)
            .appendOffset("+HH:MM", "+00:00")
            .toFormatter(Locale.US);

    private final byte[] value;
    private final byte[] metadata;
    // The variant value doesn't use the whole `value` binary, but starts from its `pos` index and
    // spans a size of `valueSize(value, pos)`. This design avoids frequent copies of the value binary
    // when reading a sub-variant in the array/object element.
    private final int position;

    public Variant(byte[] value, byte[] metadata)
    {
        this(value, metadata, 0);
    }

    private Variant(byte[] value, byte[] metadata, int position)
    {
        this.value = value;
        this.metadata = metadata;
        this.position = position;
        checkArgument(metadata.length >= 1, "metadata must be present");
        checkArgument((metadata[0] & VERSION_MASK) == VERSION, "metadata version must be %s", VERSION);
        // Don't attempt to use a Variant larger than 16 MiB. We'll never produce one, and it risks memory instability.
        checkArgument(metadata.length <= SIZE_LIMIT, "max metadata size is %s: %s", SIZE_LIMIT, metadata.length);
        checkArgument(value.length <= SIZE_LIMIT, "max value size is %s: %s", SIZE_LIMIT, value.length);
    }

    // Stringify the variant in JSON format.
    public String toJson(ZoneId zoneId)
    {
        StringBuilder json = new StringBuilder();
        toJsonImpl(value, metadata, position, json, zoneId);
        return json.toString();
    }

    private static void toJsonImpl(byte[] value, byte[] metadata, int position, StringBuilder json, ZoneId zoneId)
    {
        switch (getType(value, position)) {
            case NULL:
                json.append("null");
                break;
            case BOOLEAN:
                json.append(getBoolean(value, position));
                break;
            case LONG:
                json.append(getLong(value, position));
                break;
            case FLOAT:
                json.append(getFloat(value, position));
                break;
            case DOUBLE:
                json.append(getDouble(value, position));
                break;
            case DECIMAL:
                json.append(getDecimal(value, position).toPlainString());
                break;
            case STRING:
                json.append(escapeJson(getString(value, position)));
                break;
            case BINARY:
                appendQuoted(json, Base64.getEncoder().encodeToString(getBinary(value, position)));
                break;
            case DATE:
                appendQuoted(json, LocalDate.ofEpochDay(getLong(value, position)).toString());
                break;
            case TIMESTAMP:
                appendQuoted(json, TIMESTAMP_FORMATTER.format(microsToInstant(getLong(value, position)).atZone(zoneId)));
                break;
            case TIMESTAMP_NTZ:
                appendQuoted(json, TIMESTAMP_NTZ_FORMATTER.format(microsToInstant(getLong(value, position)).atZone(ZoneOffset.UTC)));
                break;
            case ARRAY:
                handleArray(value, position, (size, offsetSize, offsetStart, dataStart) -> {
                    json.append('[');
                    for (int i = 0; i < size; ++i) {
                        int offset = readUnsigned(value, offsetStart + offsetSize * i, offsetSize);
                        int elementPos = dataStart + offset;
                        if (i != 0) {
                            json.append(',');
                        }
                        toJsonImpl(value, metadata, elementPos, json, zoneId);
                    }
                    json.append(']');
                    return null;
                });
                break;
            case OBJECT:
                handleObject(value, position, (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    json.append('{');
                    for (int i = 0; i < size; ++i) {
                        int id = readUnsigned(value, idStart + idSize * i, idSize);
                        int offset = readUnsigned(value, offsetStart + offsetSize * i, offsetSize);
                        int elementPosition = dataStart + offset;
                        if (i != 0) {
                            json.append(',');
                        }
                        json.append(escapeJson(getMetadataKey(metadata, id)));
                        json.append(':');
                        toJsonImpl(value, metadata, elementPosition, json, zoneId);
                    }
                    json.append('}');
                    return null;
                });
                break;
        }
    }

    private static Instant microsToInstant(long timestamp)
    {
        return Instant.EPOCH.plus(timestamp, MICROS);
    }

    // A simplified and more performant version of `sb.append(escapeJson(value))`. It is used when we
    // know `value` doesn't contain any special character that needs escaping.
    private static void appendQuoted(StringBuilder json, String value)
    {
        json.append('"').append(value).append('"');
    }

    // Escape a string so that it can be pasted into JSON structure.
    // For example, if `str` only contains a new-line character, then the result content is "\n"
    // (4 characters).
    private static String escapeJson(String value)
    {
        try (CharArrayWriter writer = new CharArrayWriter();
                JsonGenerator generator = JsonFactory.builder().build().createGenerator(writer)) {
            generator.writeString(value);
            generator.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
