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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.TestPinotQueryBase;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestPinotExpressionConverters
        extends TestPinotQueryBase
{
    private final Function<VariableReferenceExpression, PinotQueryGeneratorContext.Selection> testInputFunction = testInput::get;

    @Test
    public void testProjectExpressionConverterPql()
    {
        testProjectExpressionConverter(new SessionHolder(false, false));
    }

    @Test
    public void testProjectExpressionConverterSql()
    {
        testProjectExpressionConverter(new SessionHolder(false, true));
    }

    public void testProjectExpressionConverter(SessionHolder sessionHolder)
    {
        testProject("secondssinceepoch", "secondsSinceEpoch", sessionHolder);
        // functions
        testAggregationProject(
                "date_trunc('hour', from_unixtime(secondssinceepoch))",
                "dateTimeConvert(secondsSinceEpoch, '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')",
                sessionHolder);

        // arithmetic
        testAggregationProject("regionid + 1", "ADD(regionId, 1)", sessionHolder);
        testAggregationProject("regionid - 1", "SUB(regionId, 1)", sessionHolder);
        testAggregationProject("1 * regionid", "MULT(1, regionId)", sessionHolder);
        testAggregationProject("1 / regionid", "DIV(1, regionId)", sessionHolder);

        // TODO ... this one is failing
        testAggregationProject("secondssinceepoch + 1559978258.674", "ADD(secondsSinceEpoch, 1559978258.674)", sessionHolder);

        testAggregationProject("secondssinceepoch + 1559978258", "ADD(secondsSinceEpoch, 1559978258)", sessionHolder);

        testAggregationProjectUnsupported("secondssinceepoch > 0", sessionHolder);

        testAggregationProject(
                "date_trunc('hour', from_unixtime(secondssinceepoch + 2))",
                "dateTimeConvert(ADD(secondsSinceEpoch, 2), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')",
                sessionHolder);
    }

    private void testProject(String sqlExpression, String expectedPinotExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        String actualPinotExpression = pushDownExpression.accept(
                new PinotProjectExpressionConverter(functionAndTypeManager, standardFunctionResolution),
                testInput).getDefinition();
        assertEquals(actualPinotExpression, expectedPinotExpression);
    }

    @Test
    public void testAdhocPql()
    {
        testAdhoc(new SessionHolder(false, false));
    }

    @Test
    public void testAdhocSql()
    {
        testAdhoc(new SessionHolder(false, true));
    }

    public void testAdhoc(SessionHolder sessionHolder)
    {
        testAggregationProject(
                "secondssinceepoch + 1559978258.674",
                "ADD(secondsSinceEpoch, 1559978258.674)",
                sessionHolder);
    }

    @Test
    public void testDateTruncationConversionPql()
    {
        testDateTruncationConversion(new SessionHolder(true, false));
    }

    @Test
    public void testDateTruncationConversionSql()
    {
        testDateTruncationConversion(new SessionHolder(true, true));
    }

    public void testDateTruncationConversion(SessionHolder sessionHolder)
    {
        testAggregationProject(
                "date_trunc('hour', from_unixtime(secondssinceepoch + 2))",
                "dateTrunc(ADD(secondsSinceEpoch, 2),seconds, UTC, hour)",
                sessionHolder);

        testAggregationProject(
                "date_trunc('hour', from_unixtime(secondssinceepoch + 2, 'America/New_York'))",
                "dateTrunc(ADD(secondsSinceEpoch, 2),seconds, America/New_York, hour)",
                sessionHolder);
    }

    @Test
    public void testFilterExpressionConverterPql()
    {
        testFilterExpressionConverter(new SessionHolder(false, false));
    }

    @Test
    public void testFilterExpressionConverterSql()
    {
        testFilterExpressionConverter(new SessionHolder(false, true));
    }

    public void testFilterExpressionConverter(SessionHolder sessionHolder)
    {
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

    private void testAggregationProject(String sqlExpression, String expectedPinotExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        String actualPinotExpression = pushDownExpression.accept(
                new PinotAggregationProjectConverter(
                        functionAndTypeManager,
                        functionAndTypeManager,
                        standardFunctionResolution,
                        sessionHolder.getConnectorSession()),
                testInput).getDefinition();
        assertEquals(actualPinotExpression, expectedPinotExpression);
    }

    private void testAggregationProjectUnsupported(String sqlExpression, SessionHolder sessionHolder)
    {
        try {
            RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
            String actualPinotExpression = pushDownExpression.accept(
                    new PinotAggregationProjectConverter(
                            functionAndTypeManager,
                            functionAndTypeManager,
                            standardFunctionResolution,
                            sessionHolder.getConnectorSession()),
                    testInput).getDefinition();
            fail("expected to not reach here: Generated " + actualPinotExpression);
        }
        catch (PinotException e) {
            assertEquals(e.getErrorCode(), PINOT_UNSUPPORTED_EXPRESSION.toErrorCode());
        }
    }

    private void testFilter(String sqlExpression, String expectedPinotExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        String actualPinotExpression = pushDownExpression.accept(
                new PinotFilterExpressionConverter(
                        functionAndTypeManager,
                        functionAndTypeManager,
                        standardFunctionResolution),
                testInputFunction).getDefinition();
        assertEquals(actualPinotExpression, expectedPinotExpression);
    }

    private void testFilterUnsupported(String sqlExpression, SessionHolder sessionHolder)
    {
        try {
            RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
            String actualPinotExpression = pushDownExpression.accept(
                    new PinotFilterExpressionConverter(
                            functionAndTypeManager,
                            functionAndTypeManager,
                            standardFunctionResolution),
                    testInputFunction).getDefinition();
            fail("expected to not reach here: Generated " + actualPinotExpression);
        }
        catch (PinotException e) {
            assertEquals(e.getErrorCode(), PINOT_UNSUPPORTED_EXPRESSION.toErrorCode());
        }
    }
}
