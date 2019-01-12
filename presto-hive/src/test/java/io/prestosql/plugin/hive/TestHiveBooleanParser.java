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
package io.prestosql.plugin.hive;

import org.testng.annotations.Test;

import static io.prestosql.plugin.hive.HiveBooleanParser.parseHiveBoolean;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestHiveBooleanParser
{
    @Test
    public void testParse()
    {
        assertTrue(parseBoolean("true"));
        assertTrue(parseBoolean("TRUE"));
        assertTrue(parseBoolean("tRuE"));

        assertFalse(parseBoolean("false"));
        assertFalse(parseBoolean("FALSE"));
        assertFalse(parseBoolean("fAlSe"));

        assertNull(parseBoolean("true "));
        assertNull(parseBoolean(" true"));
        assertNull(parseBoolean("false "));
        assertNull(parseBoolean(" false"));
        assertNull(parseBoolean("t"));
        assertNull(parseBoolean("f"));
        assertNull(parseBoolean(""));
        assertNull(parseBoolean("blah"));
    }

    private static Boolean parseBoolean(String s)
    {
        return parseHiveBoolean(s.getBytes(US_ASCII), 0, s.length());
    }
}
