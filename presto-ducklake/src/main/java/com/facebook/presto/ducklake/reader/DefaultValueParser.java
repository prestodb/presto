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
package com.facebook.presto.ducklake.reader;

import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.split.pruning.StatsValueParser;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.ByteArrayOutputStream;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.UUID;

import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static java.lang.String.format;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.Objects.requireNonNull;

/**
 * Converts one column's {@code initial_default} text (spec &sect;4.4) into the native value {@link
 * DefaultValuePageSource} substitutes for {@code NULL} in a file written before the column existed.
 * Unlike {@link StatsValueParser} (written for statistics pruning, where a value this connector
 * cannot represent is safely treated as "bound unknown"), a default value that cannot be parsed
 * faithfully must fail the query instead of silently reading back as {@code NULL} for every
 * pre-existing row: {@link #parse} throws {@link
 * com.facebook.presto.ducklake.DuckLakeErrorCode#DUCKLAKE_UNSUPPORTED_FEATURE} for a nested type
 * ({@code ARRAY}/{@code ROW}/{@code MAP}, which
 * DuckLake itself does not give an {@code initial_default} literal for) or for any text that fails
 * to parse for its type.
 *
 * <p>Most scalar types reuse {@link StatsValueParser#parseMin}, which already parses an exact,
 * full-precision literal correctly for those types. {@code TIME}, {@code UUID}, {@code JSON}, and
 * {@code VARBINARY} -- the types {@link StatsValueParser} cannot bound and therefore always returns
 * {@link java.util.Optional#empty()} for -- are handled directly here instead, matching the coverage
 * {@link InlinedDataPageSource} already has for the inlined-row read path.
 */
public final class DefaultValueParser
{
    private static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .toFormatter();

    private DefaultValueParser() {}

    public static Object parse(DuckLakeColumnHandle column, String defaultText)
    {
        requireNonNull(column, "column is null");
        requireNonNull(defaultText, "defaultText is null");
        Type type = column.getType();
        try {
            return parseTyped(column.getColumnIdentity().getDuckLakeType(), type, defaultText);
        }
        catch (RuntimeException e) {
            throw new PrestoException(
                    DUCKLAKE_UNSUPPORTED_FEATURE,
                    format("Default value '%s' for column '%s' of type %s is not supported", defaultText, column.getName(), type));
        }
    }

    private static Object parseTyped(String duckLakeType, Type type, String text)
    {
        if (type instanceof TimeType) {
            return parseTime(text);
        }
        if (type instanceof UuidType) {
            return UuidType.javaUuidToPrestoUuid(UUID.fromString(text.trim()));
        }
        if (type instanceof JsonType) {
            return Slices.utf8Slice(text);
        }
        if (type instanceof VarbinaryType) {
            return parseBlob(text);
        }
        // BOOLEAN, the integer types, REAL, DOUBLE, DECIMAL, DATE, every TIMESTAMP variant, and
        // VARCHAR: StatsValueParser.parseMin is exact for a full-precision literal like an
        // initial_default. Nested types (ARRAY/ROW/MAP) and anything unparseable fall through to
        // Optional.empty() and are rejected the same way here.
        return StatsValueParser.parseMin(duckLakeType, type, text)
                .orElseThrow(() -> new IllegalArgumentException(format("Could not parse default value '%s' for type %s", text, type)));
    }

    /**
     * Parses a DuckDB {@code TIME} literal ({@code HH:mm:ss} with an optional 1-9 digit fraction of
     * a second, for example {@code "13:45:30.123456"}) into milliseconds of the day, truncating any
     * sub-millisecond digits the same way the Parquet read path does.
     */
    private static long parseTime(String text)
    {
        LocalTime time = LocalTime.parse(text.trim(), TIME_FORMATTER);
        return time.toNanoOfDay() / 1_000_000L;
    }

    /**
     * Decodes a DuckDB blob literal, where every byte is either a printable ASCII character or a
     * {@code \xHH} escape (two hex digits), into a {@link Slice}. Anything else -- a non-ASCII or
     * control character, or a malformed escape -- is rejected.
     */
    private static Slice parseBlob(String text)
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(text.length());
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c == '\\') {
                if (i + 3 >= text.length() || text.charAt(i + 1) != 'x') {
                    throw new IllegalArgumentException("Invalid blob escape in: " + text);
                }
                String hex = text.substring(i + 2, i + 4);
                int value;
                try {
                    value = Integer.parseInt(hex, 16);
                }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid blob escape in: " + text, e);
                }
                bytes.write(value);
                i += 4;
            }
            else {
                if (c < 0x20 || c > 0x7E) {
                    throw new IllegalArgumentException("Invalid blob byte in: " + text);
                }
                bytes.write(c);
                i++;
            }
        }
        return Slices.wrappedBuffer(bytes.toByteArray());
    }
}
