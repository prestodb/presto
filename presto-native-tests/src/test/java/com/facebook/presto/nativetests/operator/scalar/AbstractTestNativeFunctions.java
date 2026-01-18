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
package com.facebook.presto.nativetests.operator.scalar;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.nativetests.NativeTestsUtils;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.operator.scalar.TestFunctions;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertExceptionMessage;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static org.testng.Assert.fail;

public abstract class AbstractTestNativeFunctions
        extends AbstractTestQueryFramework
        implements TestFunctions
{
    public boolean sidecarEnabled;
    private String storageFormat;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);
    }

    @Override
    public void assertFunction(String projection, Type expectedType, Object expected)
    {
        String query = format("SELECT %s", projection);
        @Language("SQL") String rewritten = rewrite(query);
        MaterializedResult result = computeActual(rewritten);
        assertEquals(result.getTypes().get(0), expectedType);
        assertEquals(result.getMaterializedRows().get(0).getField(0), expected);
    }

    @Override
    public void assertNotSupported(String projection, @Language("RegExp") String message)
    {
        String query = format("SELECT %s", projection);
        @Language("SQL") String rewritten = rewrite(query);
        try {
            computeActual(rewritten);
            fail("expected exception");
        }
        catch (RuntimeException ex) {
            assertExceptionMessage(rewritten, ex, message, true, false);
        }
    }

    /**
     * Rewrite SQL of the form 'select cast(arg as type)' to 'select cast(a as type) from (values (arg)) t(a)', and
     * SQL of the form 'select function(arg1, arg2, ...)' to
     * 'select function(a, b, ...) from (values (arg1, arg2, ...)) t(a, b, ...)'.
     * This ensures that the function is not constant-folded on the coordinator and is evaluated on the native workers.
     * Note that any arguments to the function will still be constant-folded if possible. For instance, consider the
     * SQL 'select function(a, b(c), d(e(f)), g, ...)'. Arguments such as 'b(c)' and 'd(e(f))' in the rewritten SQL
     * 'select function(p, q, r, s, ...) from (values (a, b(c), d(e(f)), g, ...)) t(p, q, r, s, ...)' can be
     * constant-folded on the coordinator. The rewrite only ensures that the top-level function call at depth 0 is not
     * evaluated on the coordinator.
     */
    public static String rewrite(String sql)
    {
        String rewrittenCast = tryRewriteCast(sql);
        if (rewrittenCast != null) {
            return rewrittenCast;
        }

        String rewrittenFunctionCall = tryRewriteFunctionCall(sql);
        if (rewrittenFunctionCall != null) {
            return rewrittenFunctionCall;
        }

        throw new IllegalArgumentException("Sql must be of form: select cast(arg as type) or select function(arg1, arg2, ...)");
    }

    /**
     * Helper function to rewrite SQL of the form 'select function(arg1, arg2, ...)' to equivalent SQL
     *          'select function(a, b, ...) from (values (arg1, arg2, ...)) t(a, b, ...)'.
     */
    private static String tryRewriteFunctionCall(String sql)
    {
        Pattern pattern = Pattern.compile(
                "^select\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\((.*)\\)\\s*$",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        if (matcher.matches()) {
            String functionName = matcher.group(1);
            String argsString = matcher.group(2);
            int depth = 0;
            boolean inSingleQuote = false;
            int numArgs = 1;
            for (int i = 0; i < argsString.length(); i++) {
                char c = argsString.charAt(i);
                if (c == '\'') {
                    inSingleQuote = !inSingleQuote;
                }
                if (!inSingleQuote) {
                    if (c == '(' || c == '[') {
                        depth++;
                    }
                    else if (c == ')' || c == ']') {
                        depth--;
                    }
                    else if (c == ',' && depth == 0) {
                        numArgs++;
                    }
                }
            }

            List<String> aliases = new ArrayList<>();
            for (int i = 0; i < numArgs; i++) {
                aliases.add(String.valueOf((char) ('a' + i)));
            }
            String aliasesString = String.join(", ", aliases);

            return format("select %s(%s) from (values (%s)) t(%s)", functionName, aliasesString, argsString, aliasesString);
        }
        return null;
    }

    /**
     * Helper function to rewrite SQL of the form 'select cast(arg as type)' to equivalent SQL
     *          'select cast(a as type) from (values (arg)) t(a)'.
     */
    private static String tryRewriteCast(String sql)
    {
        Pattern castPattern = Pattern.compile(
                "^select\\s+cast\\s*\\((.*)\\)\\s*$",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher castMatcher = castPattern.matcher(sql);
        if (castMatcher.matches()) {
            String castExpression = castMatcher.group(1).trim();
            int depth = 0;
            boolean inSingleQuote = false;

            for (int i = 0; i < castExpression.length(); i++) {
                char c = castExpression.charAt(i);
                if (c == '\'') {
                    inSingleQuote = !inSingleQuote;
                }
                if (!inSingleQuote) {
                    if (c == '(' || c == '[') {
                        depth++;
                    }
                    if (c == ')' || c == ']') {
                        depth--;
                    }
                    if (depth == 0 && i + 4 <= castExpression.length() &&
                            castExpression.substring(i, i + 4).equalsIgnoreCase(" as ")) {
                        String arg = castExpression.substring(0, i).trim();
                        String type = castExpression.substring(i + 4).trim();
                        return format("select cast(a as %s) from (values (%s)) t(a)", type, arg);
                    }
                }
            }
            throw new IllegalArgumentException("Could not parse cast expression: " + sql);
        }
        return null;
    }
}
