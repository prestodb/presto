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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.InvalidFunctionArgumentException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.lang.Character.isDigit;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@ThriftStruct
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
        try (InputStream in = TimeZoneKey.class.getResourceAsStream("zone-index.properties")) {
            // load zone file
            // todo parse file by hand since Properties ignores duplicate entries
            Properties data = new Properties()
            {
                @Override
                public synchronized Object put(Object key, Object value)
                {
                    Object existingEntry = super.put(key, value);
                    if (existingEntry != null) {
                        throw new AssertionError("Zone file has duplicate entries for " + key);
                    }
                    return null;
                }
            };
            data.load(in);

            if (data.containsKey("0")) {
                throw new AssertionError("Zone file should not contain a mapping for key 0");
            }

            Map<String, TimeZoneKey> zoneIdToKey = new TreeMap<>();
            zoneIdToKey.put(UTC_KEY.getId().toLowerCase(ENGLISH), UTC_KEY);

            short maxZoneKey = 0;
            for (Entry<Object, Object> entry : data.entrySet()) {
                short zoneKey = Short.valueOf(((String) entry.getKey()).trim());
                String zoneId = ((String) entry.getValue()).trim();

                maxZoneKey = (short) max(maxZoneKey, zoneKey);
                zoneIdToKey.put(zoneId.toLowerCase(ENGLISH), new TimeZoneKey(zoneId, zoneKey));
            }

            MAX_TIME_ZONE_KEY = maxZoneKey;
            ZONE_ID_TO_KEY = Collections.unmodifiableMap(new LinkedHashMap<>(zoneIdToKey));
            ZONE_KEYS = Collections.unmodifiableSet(new LinkedHashSet<>(zoneIdToKey.values()));

            TIME_ZONE_KEYS = new TimeZoneKey[maxZoneKey + 1];
            for (TimeZoneKey timeZoneKey : zoneIdToKey.values()) {
                TIME_ZONE_KEYS[timeZoneKey.getKey()] = timeZoneKey;
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
        checkArgument(!zoneId.isEmpty(), "Zone id is an empty string");

        TimeZoneKey zoneKey = ZONE_ID_TO_KEY.get(zoneId.toLowerCase(ENGLISH));
        if (zoneKey == null) {
            zoneKey = ZONE_ID_TO_KEY.get(normalizeZoneId(zoneId));
        }
        if (zoneKey == null) {
            throw new TimeZoneNotSupportedException(zoneId);
        }
        return zoneKey;
    }

    public static TimeZoneKey getTimeZoneKeyForOffset(long offsetMinutes)
    {
        if (offsetMinutes == 0) {
            return UTC_KEY;
        }

        if (!(offsetMinutes >= OFFSET_TIME_ZONE_MIN && offsetMinutes <= OFFSET_TIME_ZONE_MAX)) {
            throw new InvalidFunctionArgumentException(format("Invalid offset minutes %s", offsetMinutes));
        }
        TimeZoneKey timeZoneKey = OFFSET_TIME_ZONE_KEYS[((int) offsetMinutes) - OFFSET_TIME_ZONE_MIN];
        if (timeZoneKey == null) {
            throw new TimeZoneNotSupportedException(zoneIdForOffset(offsetMinutes));
        }
        return timeZoneKey;
    }

    private final String id;

    private final short key;

    @ThriftConstructor
    public TimeZoneKey(String id, short key)
    {
        this.id = requireNonNull(id, "id is null");
        if (key < 0) {
            throw new IllegalArgumentException("key is negative");
        }
        this.key = key;
    }

    @ThriftField(1)
    public String getId()
    {
        return id;
    }

    @ThriftField(2)
    @JsonValue
    public short getKey()
    {
        return key;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, key);
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
        return Objects.equals(this.id, other.id) && Objects.equals(this.key, other.key);
    }

    @Override
    public String toString()
    {
        return id;
    }

    public static boolean isUtcZoneId(String zoneId)
    {
        return normalizeZoneId(zoneId).equals("utc");
    }

    // this method is used to normalize zoneId
    private static String normalizeZoneId(String originalZoneId)
    {
        String zoneId = originalZoneId.toLowerCase(ENGLISH);

        if (!validateZoneId(zoneId)) {
            throw new TimeZoneNotSupportedException(originalZoneId);
        }

        if (!zoneId.startsWith("etc/")) {
            return originalZoneId;
        }

        zoneId = normalizeEtcGmtZoneId(zoneId);

        if (isUtcEquivalentName(zoneId) || isFixedOffsetTimeZone(zoneId)) {
            return "utc";
        }

        if (isShortOffsetTimeZone(zoneId)) {
            zoneId = normalizeShortOffset(zoneId);
        }

        if (isUtcTimeZone(originalZoneId)) {
            return flipSign(zoneId);
        }

        return zoneId;
    }

    // flip sign of zoneId
    private static String flipSign(String zoneId)
    {
        return zoneId.charAt(0) == '+' ? "-" + zoneId.substring(1) : "+" + zoneId.substring(1);
    }

    // check if zoneId is etc/utc
    private static boolean isUtcTimeZone(String zoneId)
    {
        return zoneId.contains("utc");
    }

    // check if zoneId is valid
    private static boolean validateZoneId(String zoneId)
    {
        if (zoneId.matches("etc/(gmt|greenwich|uct|universal|utc|zulu)[+-]\\d{2}:\\d{2}")) {
            zoneId = normalizeLongEtcZoneId(zoneId);
        }

        // delete sign and hour digits if zoneId is not starts with "etc/(greenwich|uct|universal|utc|zulu)"
        if (zoneId.matches("etc/(greenwich|uct|universal|utc|zulu).*")) {
            String lastNumbers = zoneId.replaceAll("etc/(greenwich|uct|universal|utc|zulu)", "");
            if (lastNumbers.length() == 3 && lastNumbers.charAt(1) == '0') {
                throw new TimeZoneNotSupportedException(zoneId);
            }

            if (Integer.parseInt(lastNumbers) < -12 || Integer.parseInt(lastNumbers) > 14) {
                throw new TimeZoneNotSupportedException(zoneId);
            }

            zoneId = zoneId.replaceAll("[+-]\\d{1,2}", "");
        }

        String finalZoneId = zoneId;
        Set<String> availableZoneIds = ZoneId.getAvailableZoneIds().stream()
                .map(id -> id.toLowerCase(ENGLISH))
                .filter(id -> id.equals(finalZoneId))
                .collect(Collectors.toSet());

        return availableZoneIds.contains(zoneId);
    }

    // normalize etc/gmt zoneId
    private static String normalizeLongEtcZoneId(String zoneId)
    {
        int colonIndex = zoneId.indexOf(':');
        if (colonIndex == -1) {
            throw new TimeZoneNotSupportedException(zoneId);
        }

        String hour = zoneId.substring(colonIndex - 2, colonIndex);

        if (hour.charAt(0) == '0') {
            hour = hour.substring(1);
        }

        return zoneId.substring(0, colonIndex - 2) + hour;
    }

    // remove etc/gmt prefix
    private static String normalizeEtcGmtZoneId(String zoneId)
    {
        return zoneId.replaceAll("etc/(gmt|greenwich|uct|universal|utc|zulu)", "");
    }

    // check if zoneId is fixed offset
    private static boolean isFixedOffsetTimeZone(String zoneId)
    {
        return "+00:00".equals(zoneId) || "-00:00".equals(zoneId);
    }

    // check if zoneId is short offset
    private static boolean isShortOffsetTimeZone(String zoneId)
    {
        return zoneId.length() == 2 || zoneId.length() == 3;
    }

    // normalize short offset zoneId
    private static String normalizeShortOffset(String zoneId)
    {
        char signChar = zoneId.charAt(0);
        if (signChar != '+' && signChar != '-') {
            return zoneId;
        }

        char hourTens = zoneId.length() == 2 ? '0' : zoneId.charAt(1);
        char hourOnes = zoneId.length() == 2 ? zoneId.charAt(1) : zoneId.charAt(2);

        if (!isDigit(hourTens) || !isDigit(hourOnes)) {
            return zoneId;
        }

        if (hourTens == '0' && hourOnes == '0') {
            return "utc";
        }

        return "" + signChar + hourTens + hourOnes + ":00";
    }

    private static boolean isUtcEquivalentName(String zoneId)
    {
        return zoneId.equals("utc") ||
                zoneId.equals("z") ||
                zoneId.equals("ut") ||
                zoneId.equals("uct") ||
                zoneId.equals("gmt") ||
                zoneId.equals("gmt0") ||
                zoneId.equals("greenwich") ||
                zoneId.equals("universal") ||
                zoneId.equals("zulu");
    }


    private static String zoneIdForOffset(long offset)
    {
        return format("%s%02d:%02d", offset < 0 ? "-" : "+", abs(offset / 60), abs(offset % 60));
    }

    private static void checkArgument(boolean check, String message, Object... args)
    {
        if (!check) {
            throw new IllegalArgumentException(format(message, args));
        }
    }
}
