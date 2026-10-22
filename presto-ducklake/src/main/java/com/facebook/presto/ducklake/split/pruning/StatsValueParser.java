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
package com.facebook.presto.ducklake.split.pruning;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateTimeEncoding;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.Objects.requireNonNull;

/**
 * Parses a {@code ducklake_file_column_stats.min_value}/{@code max_value} string into the native
 * representation of a Presto type (spec &sect;4.5, "Type Encoding for Statistics"). Every method
 * here is total: an unsupported type, an infinity/NaN marker, or any parse failure returns {@link
 * Optional#empty()} rather than throwing, so that {@link StatisticsPruner} can treat "couldn't
 * parse this bound" the same as "this bound is unknown" and keep the file. Bounds are returned
 * already rounded toward the direction that keeps pruning conservative: {@link #parseMin} floors a
 * fractional value that carries more precision than the type, {@link #parseMax} ceils one, so that
 * neither ever discards a value the stored min/max could represent.
 *
 * <p>DuckLake stores stats for a limited set of column types (see {@code
 * DuckLakeColumnStats::ToStats} in the extension): booleans, all integer types, decimal, date,
 * time, all timestamp variants, and uuid get numeric stats; float/double get stats only when
 * {@code contains_nan} is known to be false; varchar gets (possibly truncated) string stats.
 * {@code TIME} and {@code UUID} are intentionally not reproduced here (DuckDB's on-disk ordering
 * for them does not match Presto's), and BLOB/JSON/nested types have no stats at all, so all of
 * those simply fall through to {@link Optional#empty()}.
 */
public final class StatsValueParser
{
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .toFormatter();

    private static final Pattern TIMESTAMP_TZ_PATTERN = Pattern.compile(
            "(\\d{4}-\\d{2}-\\d{2}[ Tt]\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?)\\s*(Z|z|[+-]\\d{2}(?::?\\d{2})?)");

    private static final Set<String> DATE_TIME_UNBOUNDED_TOKENS = ImmutableSet.of("infinity", "-infinity", "+infinity");
    private static final Set<String> FLOAT_UNBOUNDED_TOKENS =
            ImmutableSet.of("inf", "-inf", "+inf", "infinity", "-infinity", "+infinity", "nan", "-nan", "+nan");

    private StatsValueParser() {}

    /** Parses a {@code min_value}, rounding a too-precise fractional value down. */
    public static Optional<Object> parseMin(String duckLakeType, Type type, String value)
    {
        return parse(duckLakeType, type, value, false);
    }

    /** Parses a {@code max_value}, rounding a too-precise fractional value up. */
    public static Optional<Object> parseMax(String duckLakeType, Type type, String value)
    {
        return parse(duckLakeType, type, value, true);
    }

    private static Optional<Object> parse(String duckLakeType, Type type, String value, boolean isMax)
    {
        requireNonNull(duckLakeType, "duckLakeType is null");
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        try {
            return parseTyped(type, value, isMax);
        }
        catch (RuntimeException e) {
            // Anything that fails to parse, or does not parse the way we expect, keeps the file
            // (spec ss4.5): never let a malformed or unanticipated statistics value fail the query.
            return Optional.empty();
        }
    }

    private static Optional<Object> parseTyped(Type type, String value, boolean isMax)
    {
        if (type instanceof BooleanType) {
            return parseBoolean(value);
        }
        if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType) {
            return Optional.of(Long.parseLong(value.trim()));
        }
        if (type instanceof RealType) {
            return parseReal(value);
        }
        if (type instanceof DoubleType) {
            return parseDouble(value);
        }
        if (type instanceof DecimalType) {
            return parseDecimal((DecimalType) type, value, isMax);
        }
        if (type instanceof DateType) {
            return parseDate(value);
        }
        if (type instanceof TimestampType) {
            return parseTimestamp(value, isMax);
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return parseTimestampWithTimeZone(value, isMax);
        }
        if (type instanceof VarcharType) {
            return isMax ? parseVarcharMax(value) : Optional.of(Slices.utf8Slice(value));
        }
        // TIME, UUID, JSON, VARBINARY, and structural types have no bound this connector can use.
        return Optional.empty();
    }

    private static Optional<Object> parseBoolean(String value)
    {
        String trimmed = value.trim();
        if (trimmed.equals("1") || trimmed.equalsIgnoreCase("true")) {
            return Optional.of(Boolean.TRUE);
        }
        if (trimmed.equals("0") || trimmed.equalsIgnoreCase("false")) {
            return Optional.of(Boolean.FALSE);
        }
        return Optional.empty();
    }

    private static Optional<Object> parseReal(String value)
    {
        String trimmed = value.trim();
        if (FLOAT_UNBOUNDED_TOKENS.contains(trimmed.toLowerCase(Locale.ENGLISH))) {
            return Optional.empty();
        }
        float parsed = Float.parseFloat(trimmed);
        if (Float.isInfinite(parsed) || Float.isNaN(parsed)) {
            return Optional.empty();
        }
        return Optional.of((long) Float.floatToIntBits(parsed));
    }

    private static Optional<Object> parseDouble(String value)
    {
        String trimmed = value.trim();
        if (FLOAT_UNBOUNDED_TOKENS.contains(trimmed.toLowerCase(Locale.ENGLISH))) {
            return Optional.empty();
        }
        double parsed = Double.parseDouble(trimmed);
        if (Double.isInfinite(parsed) || Double.isNaN(parsed)) {
            return Optional.empty();
        }
        return Optional.of(parsed);
    }

    private static Optional<Object> parseDecimal(DecimalType type, String value, boolean isMax)
    {
        BigDecimal parsed = new BigDecimal(value.trim());
        BigDecimal scaled = parsed.setScale(type.getScale(), isMax ? RoundingMode.CEILING : RoundingMode.FLOOR);
        if (Decimals.isShortDecimal(type)) {
            return Optional.of(scaled.unscaledValue().longValueExact());
        }
        return Optional.of(Decimals.encodeScaledValue(scaled, type.getScale()));
    }

    private static Optional<Object> parseDate(String value)
    {
        String trimmed = value.trim();
        if (DATE_TIME_UNBOUNDED_TOKENS.contains(trimmed.toLowerCase(Locale.ENGLISH))) {
            return Optional.empty();
        }
        return Optional.of(LocalDate.parse(trimmed).toEpochDay());
    }

    private static Optional<Object> parseTimestamp(String value, boolean isMax)
    {
        String trimmed = value.trim();
        if (DATE_TIME_UNBOUNDED_TOKENS.contains(trimmed.toLowerCase(Locale.ENGLISH))) {
            return Optional.empty();
        }
        LocalDateTime parsed = LocalDateTime.parse(normalizeSeparator(trimmed), TIMESTAMP_FORMATTER);
        return Optional.of(toEpochMilli(parsed, isMax));
    }

    private static Optional<Object> parseTimestampWithTimeZone(String value, boolean isMax)
    {
        String trimmed = value.trim();
        if (DATE_TIME_UNBOUNDED_TOKENS.contains(trimmed.toLowerCase(Locale.ENGLISH))) {
            return Optional.empty();
        }
        Matcher matcher = TIMESTAMP_TZ_PATTERN.matcher(trimmed);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        LocalDateTime localDateTime = LocalDateTime.parse(normalizeSeparator(matcher.group(1)), TIMESTAMP_FORMATTER);
        ZoneOffset offset = parseOffset(matcher.group(2));
        long millisUtc = OffsetDateTime.of(localDateTime, offset).toInstant().toEpochMilli();
        if (isMax && hasSubMillisecondDigits(localDateTime)) {
            millisUtc++;
        }
        return Optional.of(DateTimeEncoding.packDateTimeWithZone(millisUtc, TimeZoneKey.UTC_KEY));
    }

    private static Optional<Object> parseVarcharMax(String value)
    {
        if (value.isEmpty()) {
            // A truncated max that DuckDB reported as empty carries no usable upper bound.
            return Optional.empty();
        }
        int[] codePoints = value.codePoints().toArray();
        int lastCodePoint = codePoints[codePoints.length - 1];
        if (lastCodePoint == Character.MAX_CODE_POINT) {
            return Optional.empty();
        }
        int incremented = lastCodePoint + 1;
        if (incremented >= Character.MIN_SURROGATE && incremented <= Character.MAX_SURROGATE) {
            // A lone UTF-16 surrogate is not a valid code point: it would be re-encoded as a
            // replacement byte sequence that sorts lower than intended. Jump past the whole
            // surrogate range to the next real code point instead.
            incremented = Character.MAX_SURROGATE + 1;
        }
        codePoints[codePoints.length - 1] = incremented;
        StringBuilder builder = new StringBuilder();
        for (int codePoint : codePoints) {
            builder.appendCodePoint(codePoint);
        }
        return Optional.of(Slices.utf8Slice(builder.toString()));
    }

    private static String normalizeSeparator(String value)
    {
        return value.replace('T', ' ').replace('t', ' ');
    }

    private static long toEpochMilli(LocalDateTime value, boolean isMax)
    {
        long millis = value.toInstant(ZoneOffset.UTC).toEpochMilli();
        if (isMax && hasSubMillisecondDigits(value)) {
            millis++;
        }
        return millis;
    }

    private static boolean hasSubMillisecondDigits(LocalDateTime value)
    {
        return value.getNano() % 1_000_000 != 0;
    }

    private static ZoneOffset parseOffset(String offsetText)
    {
        if (offsetText.equalsIgnoreCase("Z")) {
            return ZoneOffset.UTC;
        }
        char sign = offsetText.charAt(0);
        String digits = offsetText.substring(1).replace(":", "");
        int hours = Integer.parseInt(digits.substring(0, 2));
        int minutes = digits.length() > 2 ? Integer.parseInt(digits.substring(2, 4)) : 0;
        int totalSeconds = (hours * 60 + minutes) * 60;
        return ZoneOffset.ofTotalSeconds(sign == '-' ? -totalSeconds : totalSeconds);
    }
}
