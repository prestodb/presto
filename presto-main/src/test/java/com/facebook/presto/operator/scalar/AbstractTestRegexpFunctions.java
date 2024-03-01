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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.RegexLibrary;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public abstract class AbstractTestRegexpFunctions
        extends AbstractTestFunctions
{
    AbstractTestRegexpFunctions(RegexLibrary regexLibrary)
    {
        super(new FeaturesConfig().setRegexLibrary(regexLibrary));
    }

    @BeforeClass
    public void setUp()
    {
        registerScalar(AbstractTestRegexpFunctions.class);
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

        // verify word boundaries at end of pattern (https://github.com/airlift/joni/pull/11)
        assertFunction("REGEXP_LIKE('test', 'test\\b')", BOOLEAN, true);
    }

    @Test
    public void testRegexCharLike()
    {
        assertFunction("REGEXP_LIKE('ala', CHAR 'ala  ')", BOOLEAN, false);
        assertFunction("REGEXP_LIKE('ala  ', CHAR 'ala  ')", BOOLEAN, true);
    }

    @Test
    public void testRegexpReplace()
    {
        assertFunction("REGEXP_REPLACE('fun stuff.', '[a-z]')", createVarcharType(10), " .");
        assertFunction("REGEXP_REPLACE('fun stuff.', '[a-z]', '*')", createVarcharType(65), "*** *****.");

        assertFunction(
                "REGEXP_REPLACE('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})')",
                createVarcharType(21),
                "call  now");
        assertFunction(
                "REGEXP_REPLACE('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})', '($1) $2-$3')",
                createVarcharType(2331),
                "call (555) 123-4444 now");

        assertFunction("REGEXP_REPLACE('xxx xxx xxx', 'x', 'x')", createVarcharType(71), "xxx xxx xxx");
        assertFunction("REGEXP_REPLACE('xxx xxx xxx', 'x', '\\x')", createVarcharType(143), "xxx xxx xxx");
        assertFunction("REGEXP_REPLACE('xxx', '', 'y')", createVarcharType(7), "yxyxyxy");
        assertInvalidFunction("REGEXP_REPLACE('xxx', 'x', '\\')", INVALID_FUNCTION_ARGUMENT);

        assertFunction("REGEXP_REPLACE('xxx xxx xxx', 'x', '$0')", createVarcharType(143), "xxx xxx xxx");
        assertFunction("REGEXP_REPLACE('xxx', '(x)', '$01')", createVarcharType(19), "xxx");
        assertFunction("REGEXP_REPLACE('xxx', 'x', '$05')", createVarcharType(19), "x5x5x5");
        assertFunction("REGEXP_REPLACE('123456789', '(1)(2)(3)(4)(5)(6)(7)(8)(9)', '$10')", createVarcharType(139), "10");
        assertFunction("REGEXP_REPLACE('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$10')", createVarcharType(175), "0");
        assertFunction("REGEXP_REPLACE('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$11')", createVarcharType(175), "11");
        assertFunction("REGEXP_REPLACE('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$1a')", createVarcharType(175), "1a");
        assertInvalidFunction("REGEXP_REPLACE('xxx', 'x', '$1')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_REPLACE('xxx', 'x', '$a')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_REPLACE('xxx', 'x', '$')", INVALID_FUNCTION_ARGUMENT);

        assertFunction("REGEXP_REPLACE('wxyz', '(?<xyz>[xyz])', '${xyz}${xyz}')", createVarcharType(124), "wxxyyzz");
        assertFunction("REGEXP_REPLACE('wxyz', '(?<w>w)|(?<xyz>[xyz])', '[${w}](${xyz})')", createVarcharType(144), "[w]()[](x)[](y)[](z)");
        assertFunction("REGEXP_REPLACE('xyz', '(?<xyz>[xyz])+', '${xyz}')", createVarcharType(39), "z");
        assertFunction("REGEXP_REPLACE('xyz', '(?<xyz>[xyz]+)', '${xyz}')", createVarcharType(39), "xyz");
        assertInvalidFunction("REGEXP_REPLACE('xxx', '(?<name>x)', '${}')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_REPLACE('xxx', '(?<name>x)', '${0}')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_REPLACE('xxx', '(?<name>x)', '${nam}')", INVALID_FUNCTION_ARGUMENT);

        assertFunction("REGEXP_REPLACE(VARCHAR 'x', '.*', 'xxxxx')", createUnboundedVarcharType(), "xxxxxxxxxx");
    }

    @Test
    public void testRegexpReplaceLambda()
    {
        // One or more matches with non-empty, not null capturing groups
        assertFunction("REGEXP_REPLACE('x', '(x)', x -> upper(x[1]))", createUnboundedVarcharType(), "X");
        assertFunction("REGEXP_REPLACE('xxx xxx xxx', '(x)', x -> upper(x[1]))", createUnboundedVarcharType(), "XXX XXX XXX");
        assertFunction("REGEXP_REPLACE('new', '(\\w)', x -> x[1])", createUnboundedVarcharType(), "new");
        assertFunction("REGEXP_REPLACE('new', '(\\w)', x -> x[1] || upper(x[1]))", createUnboundedVarcharType(), "nNeEwW");
        assertFunction("REGEXP_REPLACE('new york', '(\\w)(\\w*)', x -> upper(x[1]) || lower(x[2]))", createUnboundedVarcharType(), "New York");
        assertFunction("REGEXP_REPLACE('new york', '((\\w)(\\w*))', x -> upper(x[2]) || lower(x[3]))", createUnboundedVarcharType(), "New York");
        assertFunction("REGEXP_REPLACE('new york', '(n\\w*)', x -> upper(x[1]))", createUnboundedVarcharType(), "NEW york");
        assertFunction("REGEXP_REPLACE('new york', '(y\\w*)', x -> upper(x[1]))", createUnboundedVarcharType(), "new YORK");
        assertFunction("REGEXP_REPLACE('new york city', '(yo\\w*)', x -> upper(x[1]))", createUnboundedVarcharType(), "new YORK city");
        assertFunction("REGEXP_REPLACE('abc abc', '(abc)', x -> 'm')", createUnboundedVarcharType(), "m m");
        assertFunction("REGEXP_REPLACE('123 456', '([0-9]*)', x -> x[1])", createUnboundedVarcharType(), "123 456");
        assertFunction("REGEXP_REPLACE('123 456', '(([0-9]*) ([0-9]*))', x -> x[2] || x[3])", createUnboundedVarcharType(), "123456");
        assertFunction("REGEXP_REPLACE('abbabba', '(abba)', x -> 'm')", createUnboundedVarcharType(), "mbba");
        assertFunction("REGEXP_REPLACE('abbabba', '(abba)', x -> 'm' || x[1])", createUnboundedVarcharType(), "mabbabba");
        assertFunction("REGEXP_REPLACE('abcde', 'ab(c)?de', x -> CASE WHEN x[1] IS NULL THEN 'foo' ELSE 'bar' END)", createUnboundedVarcharType(), "bar");
        assertFunction("REGEXP_REPLACE('abc', '(.)', x -> 'm')", createUnboundedVarcharType(), "mmm");

        // Matches that contains empty capturing groups
        assertFunction("REGEXP_REPLACE('abc', '.', x -> 'm')", createUnboundedVarcharType(), "mmm");  // Empty block passed to lambda
        assertFunction("REGEXP_REPLACE('abbabba', 'abba', x -> 'm')", createUnboundedVarcharType(), "mbba"); // Empty block passed to lambda
        assertFunction("REGEXP_REPLACE('abc abc', 'abc', x -> 'm')", createUnboundedVarcharType(), "m m"); // Empty block passed to lambda
        assertFunction("REGEXP_REPLACE('abc', '', x -> 'OK')", createUnboundedVarcharType(), "OKaOKbOKcOK");  // Empty block passed to lambda
        assertFunction("REGEXP_REPLACE('abc', '()', x -> x[1])", createUnboundedVarcharType(), "abc"); // Passed a block containing multiple empty capturing groups to lambda
        assertFunction("REGEXP_REPLACE('abc', '()', x -> 'OK')", createUnboundedVarcharType(), "OKaOKbOKcOK"); // Passed a block containing multiple empty capturing groups to lambda
        assertFunction("REGEXP_REPLACE('new', '(\\w*)', x -> upper(x[1]))", createUnboundedVarcharType(), "NEW");  // Two matches: ["new"] and [""]
        assertFunction("REGEXP_REPLACE('new', '(\\w*)', x -> x[1] || upper(x[1]))", createUnboundedVarcharType(), "newNEW");  // Two matches: ["new"] and [""]
        assertFunction("REGEXP_REPLACE('new', '(\\w*)', x -> CAST(length(x[1]) AS VARCHAR))", createUnboundedVarcharType(), "30");  // Two matches: ["new"] and [""]
        assertFunction("REGEXP_REPLACE('new york', '(\\w*)', x -> '<' || x[1] || '>')", createUnboundedVarcharType(), "<new><> <york><>");  // Four matches: ["new"], [""], ["york"], [""]

        // Matches that contains null capturing groups
        assertFunction("REGEXP_REPLACE('aaa', '(b)?', x -> x[1] )", createUnboundedVarcharType(), null);
        assertFunction("REGEXP_REPLACE('abde', 'ab(c)?de', x -> x[1])", createUnboundedVarcharType(), null);  // Matched once with two matching groups[(0,3), null].
        assertFunction("REGEXP_REPLACE('abde', 'ab(c)?de', x -> 'OK')", createUnboundedVarcharType(), "OK");  // Matched once with two matching groups[(0,3), null]. Passed null to lambda and returns OK, so whole string replace with OK
        assertFunction("REGEXP_REPLACE('abde', 'ab(c)?de', x -> x[1] || 'OK')", createUnboundedVarcharType(), null); // Passed null to lambda and returns null.
        assertFunction("REGEXP_REPLACE('abde', 'ab(c)?de', x -> 'OK' || x[1])", createUnboundedVarcharType(), null);  // Passed null to lambda and returns null.
        assertFunction("REGEXP_REPLACE('abde', 'ab(c)?de', x -> CASE WHEN x[1] IS NULL THEN 'foo' ELSE 'bar' END)", createUnboundedVarcharType(), "foo");
        assertFunction("REGEXP_REPLACE('ab', '(a)?(b)?', x -> CASE WHEN (x[1] IS NOT NULL) AND (x[2] IS NOT NULL) THEN 'foo' ELSE NULL END)", createUnboundedVarcharType(), null);

        // Matches that contains non-empty and not null capturing groups but lambda returns null
        assertFunction("REGEXP_REPLACE('aaa', '(a)', x -> CAST(NULL AS VARCHAR))", createUnboundedVarcharType(), null);
        assertFunction("REGEXP_REPLACE('ab', '(a)?(b)?', x -> CASE WHEN (x[1] IS NOT NULL) AND (x[2] IS NULL) OR (x[1] IS NULL) AND (x[2] IS NOT NULL) THEN 'foo' ELSE NULL END)", createUnboundedVarcharType(), null);
        assertFunction("REGEXP_REPLACE('abacdb', '(a)?(b)?', x -> CASE WHEN (x[1] IS NOT NULL) AND (x[2] IS NULL) OR (x[1] IS NULL) AND (x[2] IS NOT NULL) THEN 'foo' ELSE NULL END)", createUnboundedVarcharType(), null);

        // No matches
        assertFunction("REGEXP_REPLACE('new york', '(a)', x -> upper(x[1]))", createUnboundedVarcharType(), "new york");
        assertFunction("REGEXP_REPLACE('', '(a)', x -> upper(x[1]))", createUnboundedVarcharType(), "");
        assertFunction("REGEXP_REPLACE(null, '(a)', x -> upper(x[1]))", createUnboundedVarcharType(), null);
        assertFunction("REGEXP_REPLACE('new', null, x -> upper(x[1]))", createUnboundedVarcharType(), null);
        assertFunction("REGEXP_REPLACE('abde', '(c)', x -> x[1])", createUnboundedVarcharType(), "abde");
        assertFunction("REGEXP_REPLACE('abde', '(c)', x -> 'm')", createUnboundedVarcharType(), "abde");

        // Invalid array indexes
        assertInvalidFunction("REGEXP_REPLACE('new', '(\\w)', x -> upper(x[2]))", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_REPLACE('new', '(\\w)', x -> upper(x[0]))", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_REPLACE('abc', '', x -> x[1])", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_REPLACE('x', 'x', x -> upper(x[1]))", INVALID_FUNCTION_ARGUMENT);  // Empty block passed to lambda but referencing an element out of bound
        assertInvalidFunction("REGEXP_REPLACE('abbabba', 'abba', x -> 'm' || x[1])", INVALID_FUNCTION_ARGUMENT);  // Empty block passed to lambda but referencing an element out of bound
    }

    @Test
    public void testRegexpExtract()
    {
        assertFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)')", createVarcharType(15), "world");
        assertFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)', 1)", createVarcharType(15), "orld");
        assertFunction("REGEXP_EXTRACT('rat cat\nbat dog', 'ra(.)|blah(.)(.)', 2)", createVarcharType(15), null);
        assertFunction("REGEXP_EXTRACT('12345', 'x')", createVarcharType(5), null);
        assertFunction("REGEXP_EXTRACT('Baby X', 'by ([A-Z].*)\\b[a-z]')", createVarcharType(6), null);

        assertInvalidFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)', -1)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REGEXP_EXTRACT('Hello world bye', '\\b[a-z]([a-z]*)', 2)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testRegexpExtractAll()
    {
        assertFunction("REGEXP_EXTRACT_ALL('rat cat\nbat dog', '.at')", new ArrayType(createVarcharType(15)), ImmutableList.of("rat", "cat", "bat"));
        assertFunction("REGEXP_EXTRACT_ALL('rat cat\nbat dog', '(.)at', 1)", new ArrayType(createVarcharType(15)), ImmutableList.of("r", "c", "b"));
        List<String> nullList = new ArrayList<>();
        nullList.add(null);
        assertFunction("REGEXP_EXTRACT_ALL('rat cat\nbat dog', 'ra(.)|blah(.)(.)', 2)", new ArrayType(createVarcharType(15)), nullList);
        assertInvalidFunction("REGEXP_EXTRACT_ALL('hello', '(.)', 2)", "Pattern has 1 groups. Cannot access group 2");

        assertFunction("REGEXP_EXTRACT_ALL('12345', '')", new ArrayType(createVarcharType(5)), ImmutableList.of("", "", "", "", "", ""));
        assertInvalidFunction("REGEXP_EXTRACT_ALL('12345', '(')", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testRegexpSplit()
    {
        assertFunction("REGEXP_SPLIT('a.b:c;d', '[\\.:;]')", new ArrayType(createVarcharType(7)), ImmutableList.of("a", "b", "c", "d"));
        assertFunction("REGEXP_SPLIT('a.b:c;d', '\\.')", new ArrayType(createVarcharType(7)), ImmutableList.of("a", "b:c;d"));
        assertFunction("REGEXP_SPLIT('a.b:c;d', ':')", new ArrayType(createVarcharType(7)), ImmutableList.of("a.b", "c;d"));
        assertFunction("REGEXP_SPLIT('a,b,c', ',')", new ArrayType(createVarcharType(5)), ImmutableList.of("a", "b", "c"));
        assertFunction("REGEXP_SPLIT('a1b2c3d', '\\d')", new ArrayType(createVarcharType(7)), ImmutableList.of("a", "b", "c", "d"));
        assertFunction("REGEXP_SPLIT('a1b2346c3d', '\\d+')", new ArrayType(createVarcharType(10)), ImmutableList.of("a", "b", "c", "d"));
        assertFunction("REGEXP_SPLIT('abcd', 'x')", new ArrayType(createVarcharType(4)), ImmutableList.of("abcd"));
        assertFunction("REGEXP_SPLIT('abcd', '')", new ArrayType(createVarcharType(4)), ImmutableList.of("", "a", "b", "c", "d", ""));
        assertFunction("REGEXP_SPLIT('', 'x')", new ArrayType(createVarcharType(0)), ImmutableList.of(""));

        // test empty splits, leading & trailing empty splits, consecutive empty splits
        assertFunction("REGEXP_SPLIT('a,b,c,d', ',')", new ArrayType(createVarcharType(7)), ImmutableList.of("a", "b", "c", "d"));
        assertFunction("REGEXP_SPLIT(',,a,,,b,c,d,,', ',')", new ArrayType(createVarcharType(13)), ImmutableList.of("", "", "a", "", "", "b", "c", "d", "", ""));
        assertFunction("REGEXP_SPLIT(',,,', ',')", new ArrayType(createVarcharType(3)), ImmutableList.of("", "", "", ""));
    }
}
