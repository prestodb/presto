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
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Boolean.parseBoolean;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.IntStream.range;

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
        String rewrittenProjection = rewriteProjection(projection);
        assertFunctionNoRewrite(rewrittenProjection, expectedType, expected);
    }

    @Override
    public void assertFunctionNoRewrite(String projection, Type expectedType, Object expected)
    {
        @Language("SQL") String query = String.format("SELECT %s", projection);
        MaterializedResult result = computeActual(query);
        assertEquals(result.getTypes().get(0), expectedType);
        assertEquals(result.getMaterializedRows().get(0).getField(0), expected);
    }

    public static String rewriteProjection(String projection)
    {
        String sql = projection.trim();
        int openParen = sql.indexOf('(');
        int closeParen = sql.lastIndexOf(')');
        if (openParen == -1 || closeParen == -1 || closeParen < openParen) {
            throw new IllegalArgumentException(String.format("Invalid projection: %s", projection));
        }

        String functionName = sql.substring(0, openParen).trim();
        if (functionName.isEmpty()) {
            throw new IllegalArgumentException(String.format("Function name missing in projection: %s", projection));
        }
        String argsString = sql.substring(openParen + 1, closeParen).trim();
        if (argsString.isEmpty()) {
            throw new IllegalArgumentException(String.format("Arguments missing for function: %s in projection: %s", functionName, projection));
        }

        List<String> args = new ArrayList<>();
        ImmutableList<Character> allowedParenthesis = ImmutableList.of('(', ')', '[', ']', '{', '}');
        Map<Character, Integer> parenthesisMap = allowedParenthesis.stream()
                .collect(Collectors.toMap(identity(), k -> 0));
        int top = 0;
        for (int i = 0; i < argsString.length(); i++) {
            char c = argsString.charAt(i);
            if (c == ',') {
                if ((parenthesisMap.get('(') + parenthesisMap.get(')')) == 0 &&
                        (parenthesisMap.get('[') + parenthesisMap.get(']')) == 0 &&
                        (parenthesisMap.get('{') + parenthesisMap.get('}')) == 0) {
                    args.add(argsString.substring(top, i).trim());
                    top = i + 1;
                }
            }
            else {
                parenthesisMap.computeIfPresent(c, (parenthesis, count) ->
                        count + (parenthesis == '(' || parenthesis == '[' || parenthesis == '{' ? 1 : -1));
            }
        }
        args.add(argsString.substring(top).trim());

        List<String> aliases = range(0, args.size())
                .mapToObj(i -> "arg" + i)
                .collect(toImmutableList());
        String aliasList = String.join(", ", aliases);
        String argList = String.join(", ", args);

        return String.format("%s(%s) from (values (%s)) t(%s)", functionName, aliasList, argList, aliasList);
    }
}
