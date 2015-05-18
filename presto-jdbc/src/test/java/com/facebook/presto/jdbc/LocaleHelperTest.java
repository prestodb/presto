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

package com.facebook.presto.jdbc;

import org.testng.annotations.Test;

import java.util.Locale;

import static com.facebook.presto.jdbc.LocaleHelper.buildLanguageTag;
import static org.testng.Assert.assertEquals;

public class LocaleHelperTest
{
    private static final Locale LOCALE_WITH_VARIANT = new Locale("aa", "bb", "cc");

    @Test
    public void testBuildLanguageTag()
    {
        assertEquals(buildLanguageTag(Locale.CANADA), "en-CA");
        assertEquals(buildLanguageTag(Locale.ENGLISH), "en");
        assertEquals(buildLanguageTag(LOCALE_WITH_VARIANT), "aa-BB-x-lvariant-cc");
    }

    @Test
    public void testDeserializeLanguageTagToLocale()
    {
        assertEquals(Locale.forLanguageTag(buildLanguageTag(LOCALE_WITH_VARIANT)), LOCALE_WITH_VARIANT);
        assertEquals(Locale.forLanguageTag(buildLanguageTag(Locale.CANADA)), Locale.CANADA);
        assertEquals(Locale.forLanguageTag(buildLanguageTag(Locale.ENGLISH)), Locale.ENGLISH);
    }
}
