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
package com.facebook.presto.operator.scalar;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRegexpFunctions
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
    }

    @Test
    public void testRegexpLike()
    {
        functionAssertions.assertFunction("REGEXP_LIKE('Stephen', 'Ste(v|ph)en')", true);
        functionAssertions.assertFunction("REGEXP_LIKE('Stevens', 'Ste(v|ph)en')", true);
        functionAssertions.assertFunction("REGEXP_LIKE('Stephen', '^Ste(v|ph)en$')", true);
        functionAssertions.assertFunction("REGEXP_LIKE('Stevens', '^Ste(v|ph)en$')", false);

        functionAssertions.assertFunction("REGEXP_LIKE('hello world', '[a-z]')", true);
        functionAssertions.assertFunction("REGEXP_LIKE('Hello', '^[a-z]+$')", false);
        functionAssertions.assertFunction("REGEXP_LIKE('Hello', '^(?i)[a-z]+$')", true);
        functionAssertions.assertFunction("REGEXP_LIKE('Hello', '^[a-zA-Z]+$')", true);
    }

    @Test
    public void testRegexpReplace()
    {
        functionAssertions.assertFunction("REGEXP_REPLACE('fun stuff.', '[a-z]')", " .");
        functionAssertions.assertFunction("REGEXP_REPLACE('fun stuff.', '[a-z]', '*')", "*** *****.");

        functionAssertions.assertFunction(
                "REGEXP_REPLACE('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})')",
                "call  now");
        functionAssertions.assertFunction(
                "REGEXP_REPLACE('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})', '($1) $2-$3')",
                "call (555) 123-4444 now");
    }

    @Test
    public void testRegexpExtract()
    {
        functionAssertions.assertFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)')", "world");
        functionAssertions.assertFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)', 1)", "orld");
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }
}
