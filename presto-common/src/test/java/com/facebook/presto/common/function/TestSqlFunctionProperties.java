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
package com.facebook.presto.common.function;

import com.facebook.presto.common.type.TimeZoneKey;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Locale;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestSqlFunctionProperties
{
    @Test
    public void testEqualsSameProperties()
    {
        SqlFunctionProperties properties1 = SqlFunctionProperties.builder()
                .setParseDecimalLiteralAsDouble(true)
                .setLegacyRowFieldOrdinalAccessEnabled(false)
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setLegacyTimestamp(false)
                .setLegacyMapSubscript(false)
                .setSessionStartTime(123456789L)
                .setSessionLocale(Locale.US)
                .setSessionUser("user1")
                .setFieldNamesInJsonCastEnabled(true)
                .setLegacyJsonCast(false)
                .setExtraCredentials(Collections.singletonMap("key", "value"))
                .setWarnOnCommonNanPatterns(false)
                .setCanonicalizedJsonExtract(true)
                .build();
        SqlFunctionProperties properties2 = SqlFunctionProperties.builder()
                .setParseDecimalLiteralAsDouble(true)
                .setLegacyRowFieldOrdinalAccessEnabled(false)
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setLegacyTimestamp(false)
                .setLegacyMapSubscript(false)
                .setSessionStartTime(123456789L)
                .setSessionLocale(Locale.US)
                .setSessionUser("user1")
                .setFieldNamesInJsonCastEnabled(true)
                .setLegacyJsonCast(false)
                .setExtraCredentials(Collections.singletonMap("key", "value"))
                .setWarnOnCommonNanPatterns(false)
                .setCanonicalizedJsonExtract(true)
                .build();
        assertEquals(properties1, properties2);
    }
    @Test
    public void testEqualsDifferentProperties()
    {
        SqlFunctionProperties properties1 = SqlFunctionProperties.builder()
                .setParseDecimalLiteralAsDouble(true)
                .setLegacyRowFieldOrdinalAccessEnabled(false)
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setLegacyTimestamp(false)
                .setLegacyMapSubscript(false)
                .setSessionStartTime(123456789L)
                .setSessionLocale(Locale.US)
                .setSessionUser("user1")
                .setFieldNamesInJsonCastEnabled(true)
                .setLegacyJsonCast(false)
                .setExtraCredentials(Collections.singletonMap("key", "value"))
                .setWarnOnCommonNanPatterns(false)
                .setCanonicalizedJsonExtract(true)
                .build();
        SqlFunctionProperties properties2 = SqlFunctionProperties.builder()
                .setParseDecimalLiteralAsDouble(true)
                .setLegacyRowFieldOrdinalAccessEnabled(false)
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setLegacyTimestamp(false)
                .setLegacyMapSubscript(false)
                .setSessionStartTime(123456789L)
                .setSessionLocale(Locale.US)
                .setSessionUser("user1")
                .setFieldNamesInJsonCastEnabled(true)
                .setLegacyJsonCast(false)
                .setExtraCredentials(Collections.singletonMap("key", "value"))
                .setWarnOnCommonNanPatterns(false)
                .setCanonicalizedJsonExtract(false) // Different value
                .build();
        assertNotEquals(properties1, properties2);
    }
}
