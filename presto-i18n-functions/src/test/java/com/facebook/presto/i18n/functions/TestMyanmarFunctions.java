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
package com.facebook.presto.i18n.functions;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;

public class TestMyanmarFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        functionAssertions.addFunctions(extractFunctions(new I18nFunctionsPlugin().getFunctions()));
    }

    @Test
    public void testMyanmarFontEncoding()
    {
        assertFunction("myanmar_font_encoding(NULL)", VARCHAR, null);
        assertFunction("myanmar_font_encoding('english string')", VARCHAR, "unicode");
        assertFunction("myanmar_font_encoding('\u1095')", VARCHAR, "zawgyi");
        assertFunction("myanmar_font_encoding('\u1021\u101E\u1004\u1039\u1038\u1019\u103D')", VARCHAR, "zawgyi");
        assertFunction("myanmar_font_encoding('\u1000\u103B\u103D\u1014\u103A\u102F\u1015\u103A')", VARCHAR, "unicode");
    }

    @Test
    public void testMyanmarNormalizeUnicode()
    {
        assertFunction("myanmar_normalize_unicode(NULL)", VARCHAR, null);
        assertFunction("myanmar_normalize_unicode('english string')", VARCHAR, "english string");
        assertFunction("myanmar_normalize_unicode('\u1021\u101E\u1004\u1039\u1038\u1019\u103D')", VARCHAR, "\u1021\u101E\u1004\u103A\u1038\u1019\u103E");
        assertFunction("myanmar_normalize_unicode('\u1000\u103B\u103D\u1014\u103A\u102F\u1015\u103A')", VARCHAR, "\u1000\u103B\u103D\u1014\u103A\u102F\u1015\u103A");
        assertFunction("myanmar_normalize_unicode('\u1000\u103B\u103D\u1014\u103A\u102F\u1015\u103A\n\u1021\u101E\u1004\u1039\u1038\u1019\u103D')", VARCHAR, "\u1000\u103B\u103D\u1014\u103A\u102F\u1015\u103A\n\u1021\u101E\u1004\u103A\u1038\u1019\u103E");
    }
}
