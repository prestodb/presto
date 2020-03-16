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
package com.facebook.presto.druid;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestDruidExpressionConverters
        extends TestDruidQueryBase
{
    private final Function<VariableReferenceExpression, DruidQueryGeneratorContext.Selection> testInputFunction = testInput::get;

    @Test
    public void testProjectExpressionConverter()
    {
        SessionHolder sessionHolder = new SessionHolder();
        testProject("secondssinceepoch", "secondsSinceEpoch", sessionHolder);
    }

    private void testProject(String sqlExpression, String expectedDruidExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        String actualDruidExpression = pushDownExpression.accept(new DruidProjectExpressionConverter(
                        typeManager,
                        standardFunctionResolution),
                testInput).getDefinition();
        assertEquals(actualDruidExpression, expectedDruidExpression);
    }

    @Test
    public void testFilterExpressionConverter()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Simple comparisons
        testFilter("regionid = 20", "(regionId = 20)", sessionHolder);
        testFilter("regionid >= 20", "(regionId >= 20)", sessionHolder);
        testFilter("city = 'Campbell'", "(city = 'Campbell')", sessionHolder);

        // between
        testFilter("totalfare between 20 and 30", "((fare + trip) BETWEEN 20 AND 30)", sessionHolder);

        // in, not in
        testFilter("regionid in (20, 30, 40)", "(regionId IN (20, 30, 40))", sessionHolder);
        testFilter("regionid not in (20, 30, 40)", "(regionId NOT IN (20, 30, 40))", sessionHolder);
        testFilter("city in ('San Jose', 'Campbell', 'Union City')", "(city IN ('San Jose', 'Campbell', 'Union City'))", sessionHolder);
        testFilter("city not in ('San Jose', 'Campbell', 'Union City')", "(city NOT IN ('San Jose', 'Campbell', 'Union City'))", sessionHolder);
        testFilterUnsupported("secondssinceepoch + 1 in (234, 24324)", sessionHolder);
        testFilterUnsupported("NOT (secondssinceepoch = 2323)", sessionHolder);

        // combinations
        testFilter("totalfare between 20 and 30 AND regionid > 20 OR city = 'Campbell'",
                "((((fare + trip) BETWEEN 20 AND 30) AND (regionId > 20)) OR (city = 'Campbell'))", sessionHolder);

        testFilter("secondssinceepoch > 1559978258", "(secondsSinceEpoch > 1559978258)", sessionHolder);
    }

    private void testFilter(String sqlExpression, String expectedDruidExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        String actualDruidExpression = pushDownExpression.accept(new DruidFilterExpressionConverter(
                        typeManager,
                        functionMetadataManager,
                        standardFunctionResolution,
                        sessionHolder.getConnectorSession()),
                testInputFunction).getDefinition();
        assertEquals(actualDruidExpression, expectedDruidExpression);
    }

    private void testFilterUnsupported(String sqlExpression, SessionHolder sessionHolder)
    {
        try {
            RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
            String actualDruidExpression = pushDownExpression.accept(new DruidFilterExpressionConverter(
                            typeManager,
                            functionMetadataManager,
                            standardFunctionResolution,
                            sessionHolder.getConnectorSession()),
                    testInputFunction).getDefinition();
            fail("expected to not reach here: Generated " + actualDruidExpression);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION.toErrorCode());
        }
    }
}
