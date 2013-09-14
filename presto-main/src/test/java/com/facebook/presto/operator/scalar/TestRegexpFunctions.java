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

import org.testng.annotations.Test;

import static com.facebook.presto.operator.scalar.FunctionAssertions.assertFunction;

public class TestRegexpFunctions
{
    @Test
    public void testRegexpLike()
    {
        assertFunction("REGEXP_LIKE('Stephen', 'Ste(v|ph)en')", true);
        assertFunction("REGEXP_LIKE('Stevens', 'Ste(v|ph)en')", true);
        assertFunction("REGEXP_LIKE('Stephen', '^Ste(v|ph)en$')", true);
        assertFunction("REGEXP_LIKE('Stevens', '^Ste(v|ph)en$')", false);

        assertFunction("REGEXP_LIKE('hello world', '[a-z]')", true);
        assertFunction("REGEXP_LIKE('Hello', '^[a-z]+$')", false);
        assertFunction("REGEXP_LIKE('Hello', '^(?i)[a-z]+$')", true);
        assertFunction("REGEXP_LIKE('Hello', '^[a-zA-Z]+$')", true);
    }

    @Test
    public void testRegexpReplace()
    {
        assertFunction("REGEXP_REPLACE('fun stuff.', '[a-z]')", " .");
        assertFunction("REGEXP_REPLACE('fun stuff.', '[a-z]', '*')", "*** *****.");

        assertFunction(
                "REGEXP_REPLACE('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})')",
                "call  now");
        assertFunction(
                "REGEXP_REPLACE('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})', '($1) $2-$3')",
                "call (555) 123-4444 now");
    }

    @Test
    public void testRegexpExtract()
    {
        assertFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)')", "world");
        assertFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)', 1)", "orld");
    }
}
