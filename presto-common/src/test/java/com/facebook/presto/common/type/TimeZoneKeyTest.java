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

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.inject.Named;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.assertj.core.api.Fail.fail;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TimeZoneKeyTest
{
    TimeZoneKey timeZoneKey = new TimeZoneKey("1", (short) 1);
    Class<?> clazz = TimeZoneKey.class;
    Method normalizeZoneId;

    public TimeZoneKeyTest()
    {
        try {
            normalizeZoneId = clazz.getDeclaredMethod("normalizeZoneId", String.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeTest
    public void setUp()
    {
        normalizeZoneId.setAccessible(true);
    }

    @Test
    @Named("GMT TEST")
    public void gmtZoneIdTest()
    {
        try {
            String zoneId = "etc/gmt+8";
            String normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "+08:00");
            zoneId = "etc/gmt-08:20";
            normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "-08:20");
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Named("UTC TEST")
    public void utcZoneIdTest()
    {
        try {
            String zoneId = "etc/utc+1";
            String normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "-01:00");

            zoneId = "etc/utc-1";
            normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "+01:00");
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Named("GREENWICH TEST")
    public void greenwichZoneIdTest()
    {
        try {
            String zoneId = "etc/greenwich+01:00";
            String normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "+01:00");

            zoneId = "etc/greenwich+6";
            normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "+06:00");

            zoneId = "etc/greenwich-6";
            normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "-06:00");

            zoneId = "etc/greenwich+11:23";
            normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "+11:23");
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Named("UNIVERSAL TEST")
    public void universalZoneIdTest()
    {
        try {
            String zoneId = "etc/universal+08:00";
            String normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "+08:00");

            zoneId = "etc/universal-08:00";
            normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "-08:00");

            zoneId = "etc/universal+11:23";
            normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "+11:23");
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Named("UCT TEST")
    public void uctZoneIdTest()
    {
        try {
            String zoneId = "etc/uct+06:00";
            String normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "+06:00");

            zoneId = "etc/uct-10:10";
            normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "-10:10");

            zoneId = "etc/uct+1";
            normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "+01:00");
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Named("ZULU TEST")
    public void zuluZoneIdTest()
    {
        try {
            String zoneId = "etc/zulu+06:00";
            String normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "+06:00");

            zoneId = "etc/zulu-10:10";
            normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "-10:10");

            zoneId = "etc/zulu+1";
            normalizedZoneId = (String) normalizeZoneId.invoke(timeZoneKey, zoneId);
            assertEquals(normalizedZoneId, "+01:00");
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Named("EXCEPTION TEST")
    public void exceptionZoneIdTest()
    {
        String[] invalidTimeZones = {"GMT-13:00", "etc/6", "etc/*", "etc/", "etc/gmt+24:00", "etc/gmt+01",
                "etc/gmt+01:000", "etc/utc+27", "etc/utc-13", "etc/gmt-16", "etc/greenwich+01",
                "etc/universal+01", "fsdkjflksdflsdlfj", "ETC/+06:00", "ETC/06:00", "ETC/+6"};

        for (String timeZone : invalidTimeZones) {
            try {
                normalizeZoneId.invoke(timeZoneKey, timeZone);
                fail("Expected TimeZoneNotSupportedException to be thrown for " + timeZone);
            }
            catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                assertTrue(cause instanceof TimeZoneNotSupportedException, "Expected TimeZoneNotSupportedException,  but got " + cause.getClass().getSimpleName());
            }
            catch (Exception e) {
                fail("Unexpected exception type: " + e.getClass().getSimpleName());
            }
        }
    }
}
