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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.spi.relation.RowExpression;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestClpFilterToKql
        extends TestClpQueryBase
{
    @Test
    public void testStringMatchPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Exact match
        testFilter("city.Name = 'hello world'", "city.Name: \"hello world\"", null, sessionHolder);
        testFilter("'hello world' = city.Name", "city.Name: \"hello world\"", null, sessionHolder);

        // Like predicates that are transformed into substring match
        testFilter("city.Name like 'hello%'", "city.Name: \"hello*\"", null, sessionHolder);
        testFilter("city.Name like '%hello'", "city.Name: \"*hello\"", null, sessionHolder);

        // Like predicates that are transformed into CARDINALITY(SPLIT(x, 'some string', 2)) = 2 form, and they are not pushed down for now
        testFilter("city.Name like '%hello%'", null, "city.Name like '%hello%'", sessionHolder);

        // Like predicates that are kept in the original forms
        testFilter("city.Name like 'hello_'", "city.Name: \"hello?\"", null, sessionHolder);
        testFilter("city.Name like '_hello'", "city.Name: \"?hello\"", null, sessionHolder);
        testFilter("city.Name like 'hello_w%'", "city.Name: \"hello?w*\"", null, sessionHolder);
        testFilter("city.Name like '%hello_w'", "city.Name: \"*hello?w\"", null, sessionHolder);
        testFilter("city.Name like 'hello%world'", "city.Name: \"hello*world\"", null, sessionHolder);
        testFilter("city.Name like 'hello%wor%ld'", "city.Name: \"hello*wor*ld\"", null, sessionHolder);
    }

    @Test
    public void testSubStringPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("substr(city.Name, 1, 2) = 'he'", "city.Name: \"he*\"", null, sessionHolder);
        testFilter("substr(city.Name, 5, 2) = 'he'", "city.Name: \"????he*\"", null, sessionHolder);
        testFilter("substr(city.Name, 5) = 'he'", "city.Name: \"????he\"", null, sessionHolder);
        testFilter("substr(city.Name, -2) = 'he'", "city.Name: \"*he\"", null, sessionHolder);

        // Invalid substring index is not pushed down
        testFilter("substr(city.Name, 1, 5) = 'he'", null, "substr(city.Name, 1, 5) = 'he'", sessionHolder);
        testFilter("substr(city.Name, -5) = 'he'", null, "substr(city.Name, -5) = 'he'", sessionHolder);
    }

    @Test
    public void testNumericComparisonPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0", "fare > 0", null, sessionHolder);
        testFilter("fare >= 0", "fare >= 0", null, sessionHolder);
        testFilter("fare < 0", "fare < 0", null, sessionHolder);
        testFilter("fare <= 0", "fare <= 0", null, sessionHolder);
        testFilter("fare = 0", "fare: 0", null, sessionHolder);
        testFilter("fare != 0", "NOT fare: 0", null, sessionHolder);
        testFilter("fare <> 0", "NOT fare: 0", null, sessionHolder);
        testFilter("0 < fare", "fare > 0", null, sessionHolder);
        testFilter("0 <= fare", "fare >= 0", null, sessionHolder);
        testFilter("0 > fare", "fare < 0", null, sessionHolder);
        testFilter("0 >= fare", "fare <= 0", null, sessionHolder);
        testFilter("0 = fare", "fare: 0", null, sessionHolder);
        testFilter("0 != fare", "NOT fare: 0", null, sessionHolder);
        testFilter("0 <> fare", "NOT fare: 0", null, sessionHolder);
    }

    @Test
    public void testOrPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0 OR city.Name like 'b%'", "(fare > 0 OR city.Name: \"b*\")", null, sessionHolder);
        testFilter(
                "lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                null,
                "(lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1)",
                sessionHolder);

        // Multiple ORs
        testFilter(
                "fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                null,
                "fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                sessionHolder);
        testFilter(
                "fare > 0 OR city.Name like 'b%' OR city.Region.Id != 1",
                "((fare > 0 OR city.Name: \"b*\") OR NOT city.Region.Id: 1)",
                null,
                sessionHolder);
    }

    @Test
    public void testAndPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0 AND city.Name like 'b%'", "(fare > 0 AND city.Name: \"b*\")", null, sessionHolder);
        testFilter(
                "lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                "(NOT city.Region.Id: 1)",
                "lower(city.Region.Name) = 'hello world'",
                sessionHolder);

        // Multiple ANDs
        testFilter(
                "fare > 0 AND city.Name like 'b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                "(((fare > 0 AND city.Name: \"b*\")) AND NOT city.Region.Id: 1)",
                "(lower(city.Region.Name) = 'hello world')",
                sessionHolder);
        testFilter(
                "fare > 0 AND city.Name like '%b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                "(((fare > 0)) AND NOT city.Region.Id: 1)",
                "city.Name like '%b%' AND lower(city.Region.Name) = 'hello world'",
                sessionHolder);
    }

    @Test
    public void testNotPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("city.Region.Name NOT LIKE 'hello%'", "NOT city.Region.Name: \"hello*\"", null, sessionHolder);
        testFilter("NOT (city.Region.Name LIKE 'hello%')", "NOT city.Region.Name: \"hello*\"", null, sessionHolder);
        testFilter("city.Name != 'hello world'", "NOT city.Name: \"hello world\"", null, sessionHolder);
        testFilter("city.Name <> 'hello world'", "NOT city.Name: \"hello world\"", null, sessionHolder);
        testFilter("NOT (city.Name = 'hello world')", "NOT city.Name: \"hello world\"", null, sessionHolder);
        testFilter("fare != 0", "NOT fare: 0", null, sessionHolder);
        testFilter("fare <> 0", "NOT fare: 0", null, sessionHolder);
        testFilter("NOT (fare = 0)", "NOT fare: 0", null, sessionHolder);

        // Multiple NOTs
        testFilter("NOT (NOT fare = 0)", "NOT NOT fare: 0", null, sessionHolder);
        testFilter("NOT (fare = 0 AND city.Name = 'hello world')", "NOT (fare: 0 AND city.Name: \"hello world\")", null, sessionHolder);
        testFilter("NOT (fare = 0 OR city.Name = 'hello world')", "NOT (fare: 0 OR city.Name: \"hello world\")", null, sessionHolder);
    }

    @Test
    public void testInPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("city.Name IN ('hello world', 'hello world 2')", "(city.Name: \"hello world\" OR city.Name: \"hello world 2\")", null, sessionHolder);
    }

    @Test
    public void testIsNullPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("city.Name IS NULL", "NOT city.Name: *", null, sessionHolder);
        testFilter("city.Name IS NOT NULL", "NOT NOT city.Name: *", null, sessionHolder);
        testFilter("NOT (city.Name IS NULL)", "NOT NOT city.Name: *", null, sessionHolder);
    }

    @Test
    public void testComplexPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(
                "(fare > 0 OR city.Name like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                "((fare > 0 OR city.Name: \"b*\"))",
                "(lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                sessionHolder);
        testFilter(
                "city.Region.Id = 1 AND (fare > 0 OR city.Name NOT like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                "((city.Region.Id: 1 AND (fare > 0 OR NOT city.Name: \"b*\")))",
                "lower(city.Region.Name) = 'hello world' OR city.Name IS NULL",
                sessionHolder);
    }

    private void testFilter(String sqlExpression, String expectedKqlExpression, String expectedRemainingExpression, SessionHolder sessionHolder)
    {
        RowExpression actualExpression = getRowExpression(sqlExpression, sessionHolder);
        ClpExpression clpExpression = actualExpression.accept(new ClpFilterToKqlConverter(standardFunctionResolution, functionAndTypeManager, variableToColumnHandleMap), null);
        Optional<String> kqlExpression = clpExpression.getPushDownExpression();
        Optional<RowExpression> remainingExpression = clpExpression.getRemainingExpression();
        if (expectedKqlExpression != null) {
            assertTrue(kqlExpression.isPresent());
            assertEquals(kqlExpression.get(), expectedKqlExpression);
        }

        if (expectedRemainingExpression != null) {
            assertTrue(remainingExpression.isPresent());
            assertEquals(remainingExpression.get(), getRowExpression(expectedRemainingExpression, sessionHolder));
        }
        else {
            assertFalse(remainingExpression.isPresent());
        }
    }
}
