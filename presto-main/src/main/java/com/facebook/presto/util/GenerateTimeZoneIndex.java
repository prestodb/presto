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
package com.facebook.presto.util;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.TimeZone;
import java.util.TreeSet;

import static com.facebook.presto.common.type.TimeZoneKey.isUtcZoneId;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Sets.filter;
import static com.google.common.collect.Sets.intersection;
import static java.lang.Math.abs;

public final class GenerateTimeZoneIndex
{
    private GenerateTimeZoneIndex()
    {
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        //
        // Header
        //
        System.out.println("#");
        System.out.println("# DO NOT REMOVE OR MODIFY EXISTING ENTRIES");
        System.out.println("#");
        System.out.println("# This file contain the fixed numeric id of every supported time zone id.");
        System.out.println("# Every zone id in this file must be supported by java.util.TimeZone and the");
        System.out.println("# Joda time library.  This is because Presto uses both java.util.TimeZone and");
        System.out.println("# the Joda time## for during execution.");
        System.out.println("#");
        System.out.println("# suppress inspection \"UnusedProperty\" for whole file");

        //
        // We assume 0 is UTC and do not generate it
        //

        //
        // Negative offset
        //
        short nextZoneKey = 1;
        for (int offset = 14 * 60; offset > 0; offset--) {
            String zoneId = String.format("-%02d:%02d", offset / 60, abs(offset % 60));

            short zoneKey = nextZoneKey++;

            System.out.println(zoneKey + " " + zoneId);
        }

        //
        // Positive offset
        //
        for (int offset = 1; offset <= 14 * 60; offset++) {
            String zoneId = String.format("+%02d:%02d", offset / 60, abs(offset % 60));

            short zoneKey = nextZoneKey++;

            System.out.println(zoneKey + " " + zoneId);
        }

        //
        // IANA regional zones: region/city
        //

        TreeSet<String> jodaZones = new TreeSet<>(DateTimeZone.getAvailableIDs());
        TreeSet<String> jdkZones = new TreeSet<>(Arrays.asList(TimeZone.getAvailableIDs()));

        TreeSet<String> zoneIds = new TreeSet<>(filter(intersection(jodaZones, jdkZones), not(ignoredZone())));

        for (String zoneId : zoneIds) {
            if (zoneId.indexOf('/') < 0) {
                continue;
            }
            short zoneKey = nextZoneKey++;

            System.out.println(zoneKey + " " + zoneId);
        }

        //
        // Other zones
        //
        for (String zoneId : zoneIds) {
            if (zoneId.indexOf('/') >= 0) {
                continue;
            }
            short zoneKey = nextZoneKey++;

            System.out.println(zoneKey + " " + zoneId);
        }

        System.out.println();
        System.out.println("# Zones not supported in Java");
        for (String invalidZone : filter(Sets.difference(jodaZones, jdkZones), not(ignoredZone()))) {
            System.out.println("# " + invalidZone);
        }

        System.out.println();
        System.out.println("# Zones not supported in Joda");
        for (String invalidZone : filter(Sets.difference(jdkZones, jodaZones), not(ignoredZone()))) {
            System.out.println("# " + invalidZone);
        }
        Thread.sleep(1000);
    }

    public static Predicate<String> ignoredZone()
    {
        return zoneId -> isUtcZoneId(zoneId) || zoneId.startsWith("Etc/");
    }
}
