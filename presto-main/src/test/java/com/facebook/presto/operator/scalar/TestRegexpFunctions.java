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

import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.RegexLibrary.JONI;
import static com.facebook.presto.sql.analyzer.RegexLibrary.RE2J;

public class TestRegexpFunctions
        extends AbstractTestFunctions
{
    private static final FeaturesConfig JONI_FEATURES_CONFIG = new FeaturesConfig()
            .setRegexLibrary(JONI);

    private static final FeaturesConfig RE2J_FEATURES_CONFIG = new FeaturesConfig()
            .setRegexLibrary(RE2J);

    {
        registerScalar(TestRegexpFunctions.class);
    }

    @Factory(dataProvider = "featuresConfig")
    public TestRegexpFunctions(FeaturesConfig featuresConfig)
    {
        super(featuresConfig);
    }

    @DataProvider(name = "featuresConfig")
    public static Object[][] featuresConfigProvider()
    {
        return new Object[][] {new Object[] {JONI_FEATURES_CONFIG}, new Object[] {RE2J_FEATURES_CONFIG}};
    }

    @ScalarFunction(deterministic = false) // if not non-deterministic, constant folding code accidentally fix invalid characters
    @SqlType(StandardTypes.VARCHAR)
    public static Slice invalidUtf8()
    {
        return Slices.wrappedBuffer(new byte[] {
                // AAA\uD800AAAA\uDFFFAAA, D800 and DFFF are valid unicode code points, but not valid UTF8
                (byte) 0x41, 0x41, (byte) 0xed, (byte) 0xa0, (byte) 0x80, 0x41, 0x41,
                0x41, 0x41, (byte) 0xed, (byte) 0xbf, (byte) 0xbf, 0x41, 0x41, 0x41,
        });
    }

    @Test
    public void testRegexpLike()
    {
        // Tests that REGEXP_LIKE doesn't loop infinitely on invalid UTF-8 input. Return value is irrelevant.
        functionAssertions.tryEvaluate("REGEXP_LIKE(invalid_utf8(), invalid_utf8())", BOOLEAN);

        assertFunction("REGEXP_LIKE('Stephen', 'Ste(v|ph)en')", BOOLEAN, true);
        assertFunction("REGEXP_LIKE('Stevens', 'Ste(v|ph)en')", BOOLEAN, true);
        assertFunction("REGEXP_LIKE('Stephen', '^Ste(v|ph)en$')", BOOLEAN, true);
        assertFunction("REGEXP_LIKE('Stevens', '^Ste(v|ph)en$')", BOOLEAN, false);

        assertFunction("REGEXP_LIKE('hello world', '[a-z]')", BOOLEAN, true);
        assertFunction("REGEXP_LIKE('hello\nworld', '.*hello\nworld.*')", BOOLEAN, true);
        assertFunction("REGEXP_LIKE('Hello', '^[a-z]+$')", BOOLEAN, false);
        assertFunction("REGEXP_LIKE('Hello', '^(?i)[a-z]+$')", BOOLEAN, true);
        assertFunction("REGEXP_LIKE('Hello', '^[a-zA-Z]+$')", BOOLEAN, true);
    }

    @Test
    public void testRegexpReplace()
    {
        assertFunction("REGEXP_REPLACE('fun stuff.', '[a-z]')", VARCHAR, " .");
        assertFunction("REGEXP_REPLACE('fun stuff.', '[a-z]', '*')", VARCHAR, "*** *****.");

        assertFunction(
                "REGEXP_REPLACE('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})')",
                VARCHAR,
                "call  now");
        assertFunction(
                "REGEXP_REPLACE('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})', '($1) $2-$3')",
                VARCHAR,
                "call (555) 123-4444 now");

        assertFunction("REGEXP_REPLACE('xxx xxx xxx', 'x', 'x')", VARCHAR, "xxx xxx xxx");
        assertFunction("REGEXP_REPLACE('xxx xxx xxx', 'x', '\\x')", VARCHAR, "xxx xxx xxx");
        assertFunction("REGEXP_REPLACE('xxx', '', 'y')", VARCHAR, "yxyxyxy");
        assertInvalidFunction("REGEXP_REPLACE('xxx', 'x', '\\')", INVALID_FUNCTION_ARGUMENT);

        assertFunction("REGEXP_REPLACE('xxx xxx xxx', 'x', '$0')", VARCHAR, "xxx xxx xxx");
        assertFunction("REGEXP_REPLACE('xxx', '(x)', '$01')", VARCHAR, "xxx");
        assertFunction("REGEXP_REPLACE('xxx', 'x', '$05')", VARCHAR, "x5x5x5");
        assertFunction("REGEXP_REPLACE('123456789', '(1)(2)(3)(4)(5)(6)(7)(8)(9)', '$10')", VARCHAR, "10");
        assertFunction("REGEXP_REPLACE('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$10')", VARCHAR, "0");
        assertFunction("REGEXP_REPLACE('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$11')", VARCHAR, "11");
        assertFunction("REGEXP_REPLACE('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$1a')", VARCHAR, "1a");
        assertInvalidFunction("REGEXP_REPLACE('xxx', 'x', '$1')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_REPLACE('xxx', 'x', '$a')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_REPLACE('xxx', 'x', '$')", INVALID_FUNCTION_ARGUMENT);

        assertFunction("REGEXP_REPLACE('wxyz', '(?<xyz>[xyz])', '${xyz}${xyz}')", VARCHAR, "wxxyyzz");
        assertFunction("REGEXP_REPLACE('wxyz', '(?<w>w)|(?<xyz>[xyz])', '[${w}](${xyz})')", VARCHAR, "[w]()[](x)[](y)[](z)");
        assertFunction("REGEXP_REPLACE('xyz', '(?<xyz>[xyz])+', '${xyz}')", VARCHAR, "z");
        assertFunction("REGEXP_REPLACE('xyz', '(?<xyz>[xyz]+)', '${xyz}')", VARCHAR, "xyz");
        assertInvalidFunction("REGEXP_REPLACE('xxx', '(?<name>x)', '${}')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_REPLACE('xxx', '(?<name>x)', '${0}')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_REPLACE('xxx', '(?<name>x)', '${nam}')", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testRegexpExtract()
    {
        assertFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)')", VARCHAR, "world");
        assertFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)', 1)", VARCHAR, "orld");
        assertFunction("REGEXP_EXTRACT('rat cat\nbat dog', 'ra(.)|blah(.)(.)', 2)", VARCHAR, null);
        assertFunction("REGEXP_EXTRACT('12345', 'x')", VARCHAR, null);

        assertInvalidFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)', -1)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)', 2)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testRegexpExtractAll()
    {
        assertFunction("REGEXP_EXTRACT_ALL('rat cat\nbat dog', '.at')", new ArrayType(VARCHAR), ImmutableList.of("rat", "cat", "bat"));
        assertFunction("REGEXP_EXTRACT_ALL('rat cat\nbat dog', '(.)at', 1)", new ArrayType(VARCHAR), ImmutableList.of("r", "c", "b"));
        List<String> nullList = new ArrayList<>();
        nullList.add(null);
        assertFunction("REGEXP_EXTRACT_ALL('rat cat\nbat dog', 'ra(.)|blah(.)(.)', 2)", new ArrayType(VARCHAR), nullList);
        assertInvalidFunction("REGEXP_EXTRACT_ALL('hello', '(.)', 2)", "Pattern has 1 groups. Cannot access group 2");

        assertFunction("REGEXP_EXTRACT_ALL('12345', '')", new ArrayType(VARCHAR), ImmutableList.of("", "", "", "", "", ""));
        assertInvalidFunction("REGEXP_EXTRACT_ALL('12345', '(')", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testRegexpSplit()
    {
        assertFunction("REGEXP_SPLIT('a.b:c;d', '[\\.:;]')", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c", "d"));
        assertFunction("REGEXP_SPLIT('a.b:c;d', '\\.')", new ArrayType(VARCHAR), ImmutableList.of("a", "b:c;d"));
        assertFunction("REGEXP_SPLIT('a.b:c;d', ':')", new ArrayType(VARCHAR), ImmutableList.of("a.b", "c;d"));
        assertFunction("REGEXP_SPLIT('a,b,c', ',')", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c"));
        assertFunction("REGEXP_SPLIT('a1b2c3d', '\\d')", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c", "d"));
        assertFunction("REGEXP_SPLIT('a1b2346c3d', '\\d+')", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c", "d"));
        assertFunction("REGEXP_SPLIT('abcd', 'x')", new ArrayType(VARCHAR), ImmutableList.of("abcd"));
        assertFunction("REGEXP_SPLIT('abcd', '')", new ArrayType(VARCHAR), ImmutableList.of("", "a", "b", "c", "d", ""));
        assertFunction("REGEXP_SPLIT('', 'x')", new ArrayType(VARCHAR), ImmutableList.of(""));

        // test empty splits, leading & trailing empty splits, consecutive empty splits
        assertFunction("REGEXP_SPLIT('a,b,c,d', ',')", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c", "d"));
        assertFunction("REGEXP_SPLIT(',,a,,,b,c,d,,', ',')", new ArrayType(VARCHAR), ImmutableList.of("", "", "a", "", "", "b", "c", "d", "", ""));
        assertFunction("REGEXP_SPLIT(',,,', ',')", new ArrayType(VARCHAR), ImmutableList.of("", "", "", ""));
    }
}
