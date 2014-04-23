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
package com.facebook.presto.spi.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public final class TimeZoneKey
{
    public static final TimeZoneKey UTC_KEY = new TimeZoneKey("UTC", (short) 0);
    public static final short MAX_TIME_ZONE_KEY;
    private static final Map<String, TimeZoneKey> ZONE_ID_TO_KEY;
    private static final Set<TimeZoneKey> ZONE_KEYS;

    private static final TimeZoneKey[] TIME_ZONE_KEYS;

    private static final short OFFSET_TIME_ZONE_MIN = -14 * 60;
    private static final short OFFSET_TIME_ZONE_MAX = 14 * 60;
    private static final TimeZoneKey[] OFFSET_TIME_ZONE_KEYS = new TimeZoneKey[OFFSET_TIME_ZONE_MAX - OFFSET_TIME_ZONE_MIN + 1];

    static {
        try (InputStream in = TimeZoneIndex.class.getResourceAsStream("zone-index.properties")) {
            // load zone file
            Properties data = new Properties();
            data.load(in);

            if (data.containsKey("0")) {
                throw new AssertionError("Zone file should not contain a mapping for key 0");
            }

            Map<String, TimeZoneKey> zoneIdToKey = new TreeMap<>();
            zoneIdToKey.put(UTC_KEY.getTimeZoneId(), UTC_KEY);

            short maxZoneKey = 0;
            for (Entry<Object, Object> entry : data.entrySet()) {
                short zoneKey = Short.valueOf(((String) entry.getKey()).trim());
                String zoneId = ((String) entry.getValue()).trim();

                maxZoneKey = (short) max(maxZoneKey, zoneKey);
                zoneIdToKey.put(zoneId, new TimeZoneKey(zoneId, zoneKey));
            }

            MAX_TIME_ZONE_KEY = maxZoneKey;
            ZONE_ID_TO_KEY = Collections.unmodifiableMap(new LinkedHashMap<>(zoneIdToKey));
            ZONE_KEYS = Collections.unmodifiableSet(new LinkedHashSet<>(zoneIdToKey.values()));

            TIME_ZONE_KEYS = new TimeZoneKey[maxZoneKey + 1];
            for (TimeZoneKey timeZoneKey : zoneIdToKey.values()) {
                TIME_ZONE_KEYS[timeZoneKey.getTimeZoneKey()] = timeZoneKey;
            }

            for (short offset = OFFSET_TIME_ZONE_MIN; offset <= OFFSET_TIME_ZONE_MAX; offset++) {
                if (offset == 0) {
                    continue;
                }
                String zoneId = zoneIdForOffset(offset);
                TimeZoneKey zoneKey = ZONE_ID_TO_KEY.get(zoneId);
                OFFSET_TIME_ZONE_KEYS[offset - OFFSET_TIME_ZONE_MIN] = zoneKey;
            }
        }
        catch (IOException e) {
            throw new AssertionError("Error loading time zone index file", e);
        }
    }

    public static Set<TimeZoneKey> getTimeZoneKeys()
    {
        return ZONE_KEYS;
    }

    @JsonCreator
    public static TimeZoneKey getTimeZoneKey(short timeZoneKey)
    {
        checkArgument(timeZoneKey < TIME_ZONE_KEYS.length && TIME_ZONE_KEYS[timeZoneKey] != null, "Invalid time zone key %d", timeZoneKey);
        return TIME_ZONE_KEYS[timeZoneKey];
    }

    public static TimeZoneKey getTimeZoneKey(String zoneId)
    {
        requireNonNull(zoneId, "Zone id is null");

        TimeZoneKey zoneKey = ZONE_ID_TO_KEY.get(zoneId);
        if (zoneKey == null) {
            zoneKey = ZONE_ID_TO_KEY.get(normalizeZoneId(zoneId));
        }
        if (zoneKey == null) {
            throw new TimeZoneNotSupported(zoneId);
        }
        return zoneKey;
    }

    public static TimeZoneKey getTimeZoneKeyForOffset(long offsetMinutes)
    {
        if (offsetMinutes == 0) {
            return UTC_KEY;
        }

        checkArgument(offsetMinutes >= OFFSET_TIME_ZONE_MIN && offsetMinutes <= OFFSET_TIME_ZONE_MAX, "Invalid offset minutes %s", offsetMinutes);
        TimeZoneKey timeZoneKey = OFFSET_TIME_ZONE_KEYS[((int) offsetMinutes) - OFFSET_TIME_ZONE_MIN];
        if (timeZoneKey == null) {
            throw new TimeZoneNotSupported(zoneIdForOffset(offsetMinutes));
        }
        return timeZoneKey;
    }

    private final String timeZoneId;

    private final short timeZoneKey;
    TimeZoneKey(String timeZoneId, short timeZoneKey)
    {
        this.timeZoneId = requireNonNull(timeZoneId, "timeZoneId is null");
        if (timeZoneKey < 0) {
            throw new IllegalArgumentException("timeZoneKey is negative");
        }
        this.timeZoneKey = timeZoneKey;
    }

    public String getTimeZoneId()
    {
        return timeZoneId;
    }

    @JsonValue
    public short getTimeZoneKey()
    {
        return timeZoneKey;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(timeZoneId, timeZoneKey);
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
        TimeZoneKey other = (TimeZoneKey) obj;
        return Objects.equals(this.timeZoneId, other.timeZoneId) && Objects.equals(this.timeZoneKey, other.timeZoneKey);
    }

    @Override
    public String toString()
    {
        return timeZoneId;
    }

    public static boolean isUtcZoneId(String zoneId)
    {
        return zoneId.equals("Z") ||
                zoneId.equals("UTC") ||
                zoneId.equals("UCT") ||
                zoneId.equals("UT") ||
                zoneId.equals("GMT") ||
                zoneId.equals("GMTO") ||
                zoneId.equals("Greenwich") ||
                zoneId.equals("Universal") ||
                zoneId.equals("Zulu");
    }

    private static String normalizeZoneId(String zoneId)
    {
        if (zoneId.startsWith("Etc/")) {
            zoneId = zoneId.substring(4);
        }

        if (isUtcZoneId(zoneId)) {
            return "UTC";
        }

        int length = zoneId.length();
        if (length > 3 && (zoneId.startsWith("UTC") || zoneId.startsWith("GMT"))) {
            zoneId = zoneId.substring(3);
        }
        if (length > 2 && zoneId.startsWith("UT")) {
            zoneId = zoneId.substring(2);
        }
        if (length == 2 || length == 3) {
            char signChar = zoneId.charAt(0);
            if (signChar != '+' && signChar != '-') {
                return zoneId;
            }

            char hourTens;
            char hourOnes;
            if (length == 2) {
                hourTens = '0';
                hourOnes = zoneId.charAt(1);
            }
            else {
                hourTens = zoneId.charAt(1);
                hourOnes = zoneId.charAt(2);
            }

            if ((hourTens == '0' || hourTens == '1') && (hourOnes >= '0' || '9' >= hourOnes)) {
                zoneId = "" + signChar + hourTens + hourOnes + ":00";
            }

        }

        if (zoneId.equals("+00:00") || zoneId.equals("-00:00")) {
            return "UTC";
        }
        return zoneId;
    }

    private static String zoneIdForOffset(long offset)
    {
        return String.format("%s%02d:%02d", offset < 0 ? "-" : "+", abs(offset / 60), abs(offset % 60));
    }

    private static void checkArgument(boolean check, String message, Object... args)
    {
        if (!check) {
            throw new IllegalArgumentException(String.format(message, args));
        }
    }
}
