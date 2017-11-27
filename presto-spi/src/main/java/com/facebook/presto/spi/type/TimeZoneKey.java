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

import com.facebook.presto.spi.PrestoException;
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

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Character.isDigit;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.util.Locale.ENGLISH;
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
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, String.format("Invalid offset minutes %s", offsetMinutes));
        }
        TimeZoneKey timeZoneKey = OFFSET_TIME_ZONE_KEYS[((int) offsetMinutes) - OFFSET_TIME_ZONE_MIN];
        if (timeZoneKey == null) {
            throw new TimeZoneNotSupportedException(zoneIdForOffset(offsetMinutes));
        }
        return timeZoneKey;
    }

    private final String id;

    private final short key;

    TimeZoneKey(String id, short key)
    {
        this.id = requireNonNull(id, "id is null");
        if (key < 0) {
            throw new IllegalArgumentException("key is negative");
        }
        this.key = key;
    }

    public String getId()
    {
        return id;
    }

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

    private static String normalizeZoneId(String originalZoneId)
    {
        String zoneId = originalZoneId.toLowerCase(ENGLISH);

        boolean startsWithEtc = zoneId.startsWith("etc/");
        if (startsWithEtc) {
            zoneId = zoneId.substring(4);
        }

        if (isUtcEquivalentName(zoneId)) {
            return "utc";
        }

        //
        // Normalize fixed offset time zones.
        //

        // In some zones systems, these will start with UTC, GMT or UT.
        int length = zoneId.length();
        boolean startsWithEtcGmt = false;
        if (length > 3 && (zoneId.startsWith("utc") || zoneId.startsWith("gmt"))) {
            if (startsWithEtc && zoneId.startsWith("gmt")) {
                startsWithEtcGmt = true;
            }
            zoneId = zoneId.substring(3);
            length = zoneId.length();
        }
        else if (length > 2 && zoneId.startsWith("ut")) {
            zoneId = zoneId.substring(2);
            length = zoneId.length();
        }

        // (+/-)00:00 is UTC
        if ("+00:00".equals(zoneId) || "-00:00".equals(zoneId)) {
            return "utc";
        }

        // if zoneId matches XXX:XX, it is likely +HH:mm, so just return it
        // since only offset time zones will contain a `:` character
        if (length == 6 && zoneId.charAt(3) == ':') {
            return zoneId;
        }

        //
        // Rewrite (+/-)H[H] to (+/-)HH:00
        //
        if (length != 2 && length != 3) {
            return originalZoneId;
        }

        // zone must start with a plus or minus sign
        char signChar = zoneId.charAt(0);
        if (signChar != '+' && signChar != '-') {
            return originalZoneId;
        }
        if (startsWithEtcGmt) {
            // Flip sign for Etc/GMT(+/-)H[H]
            signChar = signChar == '-' ? '+' : '-';
        }

        // extract the tens and ones characters for the hour
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

        // do we have a valid hours offset time zone?
        if (!isDigit(hourTens) || !isDigit(hourOnes)) {
            return originalZoneId;
        }

        // is this offset 0 (e.g., UTC)?
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
                zoneId.equals("ut") ||
                zoneId.equals("gmt") ||
                zoneId.equals("gmt0") ||
                zoneId.equals("greenwich") ||
                zoneId.equals("universal") ||
                zoneId.equals("zulu");
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
