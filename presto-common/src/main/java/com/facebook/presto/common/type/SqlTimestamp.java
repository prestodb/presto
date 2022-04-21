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
package com.facebook.presto.common.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class SqlTimestamp
{
    private static final long MICROS_PER_SECOND = 1_000_000;
    private static final long NANOS_PER_MICROS = 1_000;

    // This needs to be Locale-independent, Java Time's DateTimeFormatter compatible and should never change, as it defines the external API data format.
    private static final String JSON_MILLIS_FORMAT = "uuuu-MM-dd HH:mm:ss.SSS";
    public static final DateTimeFormatter JSON_MILLIS_FORMATTER = DateTimeFormatter.ofPattern(JSON_MILLIS_FORMAT);

    private static final String JSON_MICROS_FORMAT = "uuuu-MM-dd HH:mm:ss.SSSSSS";
    public static final DateTimeFormatter JSON_MICROS_FORMATTER = DateTimeFormatter.ofPattern(JSON_MICROS_FORMAT);

    private final long value;
    private final Optional<TimeZoneKey> sessionTimeZoneKey;
    private final TimeUnit precision;

    public SqlTimestamp(long value, TimeUnit precision)
    {
        this.value = value;
        sessionTimeZoneKey = Optional.empty();
        this.precision = validatePrecision(precision);
    }

    @Deprecated
    public SqlTimestamp(long valueUTC, TimeZoneKey sessionTimeZoneKey, TimeUnit precision)
    {
        this.value = valueUTC;
        this.sessionTimeZoneKey = Optional.of(sessionTimeZoneKey);
        this.precision = validatePrecision(precision);
    }

    public long getMillis()
    {
        checkState(!isLegacyTimestamp(), "getMillis() can be called in new timestamp semantics only");
        return precision.toMillis(value);
    }

    @Deprecated
    public long getMillisUtc()
    {
        checkState(isLegacyTimestamp(), "getMillisUtc() can be called in legacy timestamp semantics only");
        return precision.toMillis(value);
    }

    @Deprecated
    public Optional<TimeZoneKey> getSessionTimeZoneKey()
    {
        return sessionTimeZoneKey;
    }

    @Deprecated
    public boolean isLegacyTimestamp()
    {
        return sessionTimeZoneKey.isPresent();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, precision, sessionTimeZoneKey);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlTimestamp other = (SqlTimestamp) obj;
        // The current semantics returns NOT equal for millis and micros timestamp,
        // even though they represent the same time. (ex. same second, millis/micros set to 0).
        return this.value == other.value &&
                this.precision == other.precision &&
                Objects.equals(this.sessionTimeZoneKey, other.sessionTimeZoneKey);
    }

    @JsonValue
    @Override
    public String toString()
    {
        if (precision == MILLISECONDS) {
            return formatInstant(millisToInstant(value), JSON_MILLIS_FORMATTER);
        }
        if (precision == MICROSECONDS) {
            return formatInstant(microsToInstant(value), JSON_MICROS_FORMATTER);
        }
        throw new UnsupportedOperationException("Precision not supported " + precision);
    }

    private String formatInstant(Instant instant, DateTimeFormatter formatter)
    {
        if (isLegacyTimestamp()) {
            return instant.atZone(ZoneId.of(sessionTimeZoneKey.get().getId())).format(formatter);
        }
        else {
            return instant.atZone(ZoneId.of(UTC_KEY.getId())).format(formatter);
        }
    }

    private static TimeUnit validatePrecision(TimeUnit precision)
    {
        requireNonNull(precision, "precision");
        if (precision == MILLISECONDS || precision == MICROSECONDS) {
            return precision;
        }
        throw new UnsupportedOperationException("Precision not supported " + precision);
    }

    private static Instant millisToInstant(long epochMillis)
    {
        return Instant.ofEpochMilli(epochMillis);
    }

    private static Instant microsToInstant(long epochMicros)
    {
        long seconds = floorDiv(epochMicros, MICROS_PER_SECOND);
        long micros = floorMod(epochMicros, MICROS_PER_SECOND);
        return Instant.ofEpochSecond(seconds, micros * NANOS_PER_MICROS);
    }

    private static void checkState(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
