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
public class TestClpPlanOptimizer
        extends TestClpQueryBase
{
    private void testFilter(String sqlExpression, Optional<String> expectedKqlExpression,
                            Optional<String> expectedRemainingExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        ClpExpression clpExpression = pushDownExpression.accept(new ClpFilterToKqlConverter(
                        standardFunctionResolution,
                        functionAndTypeManager,
                        variableToColumnHandleMap),
                null);
        Optional<String> kqlExpression = clpExpression.getDefinition();
        Optional<RowExpression> remainingExpression = clpExpression.getRemainingExpression();
        if (expectedKqlExpression.isPresent()) {
            assertTrue(kqlExpression.isPresent());
            assertEquals(kqlExpression.get(), expectedKqlExpression.get());
        }

        if (expectedRemainingExpression.isPresent()) {
            assertTrue(remainingExpression.isPresent());
            assertEquals(remainingExpression.get(), getRowExpression(expectedRemainingExpression.get(), sessionHolder));
        }
        else {
            assertFalse(remainingExpression.isPresent());
        }
    }

    @Test
    public void testStringMatchPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Exact match
        testFilter("city.Name = 'hello world'", Optional.of("city.Name: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("'hello world' = city.Name", Optional.of("city.Name: \"hello world\""), Optional.empty(), sessionHolder);

        // Like predicates that are transformed into substring match
        testFilter("city.Name like 'hello%'", Optional.of("city.Name: \"hello*\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like '%hello'", Optional.of("city.Name: \"*hello\""), Optional.empty(), sessionHolder);

        // Like predicates that are transformed into CARDINALITY(SPLIT(x, 'some string', 2)) = 2 form, and they are not pushed down for now
        testFilter("city.Name like '%hello%'", Optional.empty(), Optional.of("city.Name like '%hello%'"), sessionHolder);

        // Like predicates that are kept in the original forms
        testFilter("city.Name like 'hello_'", Optional.of("city.Name: \"hello?\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like '_hello'", Optional.of("city.Name: \"?hello\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like 'hello_w%'", Optional.of("city.Name: \"hello?w*\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like '%hello_w'", Optional.of("city.Name: \"*hello?w\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like 'hello%world'", Optional.of("city.Name: \"hello*world\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like 'hello%wor%ld'", Optional.of("city.Name: \"hello*wor*ld\""), Optional.empty(), sessionHolder);
    }

    @Test
    public void testSubStringPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("substr(city.Name, 1, 2) = 'he'", Optional.of("city.Name: \"he*\""), Optional.empty(), sessionHolder);
        testFilter("substr(city.Name, 5, 2) = 'he'", Optional.of("city.Name: \"????he*\""), Optional.empty(), sessionHolder);
        testFilter("substr(city.Name, 5) = 'he'", Optional.of("city.Name: \"????he\""), Optional.empty(), sessionHolder);
        testFilter("substr(city.Name, -2) = 'he'", Optional.of("city.Name: \"*he\""), Optional.empty(), sessionHolder);

        // Invalid substring index is not pushed down
        testFilter("substr(city.Name, 1, 5) = 'he'", Optional.empty(), Optional.of("substr(city.Name, 1, 5) = 'he'"), sessionHolder);
        testFilter("substr(city.Name, -5) = 'he'", Optional.empty(), Optional.of("substr(city.Name, -5) = 'he'"), sessionHolder);
    }

    @Test
    public void testNumericComparisonPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0", Optional.of("fare > 0"), Optional.empty(), sessionHolder);
        testFilter("fare >= 0", Optional.of("fare >= 0"), Optional.empty(), sessionHolder);
        testFilter("fare < 0", Optional.of("fare < 0"), Optional.empty(), sessionHolder);
        testFilter("fare <= 0", Optional.of("fare <= 0"), Optional.empty(), sessionHolder);
        testFilter("fare = 0", Optional.of("fare: 0"), Optional.empty(), sessionHolder);
        testFilter("fare != 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("fare <> 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("0 < fare", Optional.of("fare > 0"), Optional.empty(), sessionHolder);
        testFilter("0 <= fare", Optional.of("fare >= 0"), Optional.empty(), sessionHolder);
        testFilter("0 > fare", Optional.of("fare < 0"), Optional.empty(), sessionHolder);
        testFilter("0 >= fare", Optional.of("fare <= 0"), Optional.empty(), sessionHolder);
        testFilter("0 = fare", Optional.of("fare: 0"), Optional.empty(), sessionHolder);
        testFilter("0 != fare", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("0 <> fare", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testOrPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0 OR city.Name like 'b%'", Optional.of("(fare > 0 OR city.Name: \"b*\")"), Optional.empty(),
                sessionHolder);
        testFilter("lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1", Optional.empty(), Optional.of("(lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1)"),
                sessionHolder);

        // Multiple ORs
        testFilter("fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                Optional.empty(),
                Optional.of("fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1"),
                sessionHolder);
        testFilter("fare > 0 OR city.Name like 'b%' OR city.Region.Id != 1",
                Optional.of("((fare > 0 OR city.Name: \"b*\") OR NOT city.Region.Id: 1)"),
                Optional.empty(),
                sessionHolder);
    }

    @Test
    public void testAndPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0 AND city.Name like 'b%'", Optional.of("(fare > 0 AND city.Name: \"b*\")"), Optional.empty(), sessionHolder);
        testFilter("lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1", Optional.of("(NOT city.Region.Id: 1)"), Optional.of("lower(city.Region.Name) = 'hello world'"),
                sessionHolder);

        // Multiple ANDs
        testFilter("fare > 0 AND city.Name like 'b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                Optional.of("(((fare > 0 AND city.Name: \"b*\")) AND NOT city.Region.Id: 1)"),
                Optional.of("(lower(city.Region.Name) = 'hello world')"),
                sessionHolder);
        testFilter("fare > 0 AND city.Name like '%b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                Optional.of("(((fare > 0)) AND NOT city.Region.Id: 1)"),
                Optional.of("city.Name like '%b%' AND lower(city.Region.Name) = 'hello world'"),
                sessionHolder);
    }

    @Test
    public void testNotPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("city.Region.Name NOT LIKE 'hello%'", Optional.of("NOT city.Region.Name: \"hello*\""), Optional.empty(), sessionHolder);
        testFilter("NOT (city.Region.Name LIKE 'hello%')", Optional.of("NOT city.Region.Name: \"hello*\""), Optional.empty(), sessionHolder);
        testFilter("city.Name != 'hello world'", Optional.of("NOT city.Name: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("city.Name <> 'hello world'", Optional.of("NOT city.Name: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("NOT (city.Name = 'hello world')", Optional.of("NOT city.Name: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("fare != 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("fare <> 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("NOT (fare = 0)", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);

        // Multiple NOTs
        testFilter("NOT (NOT fare = 0)", Optional.of("NOT NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("NOT (fare = 0 AND city.Name = 'hello world')", Optional.of("NOT (fare: 0 AND city.Name: \"hello world\")"), Optional.empty(), sessionHolder);
        testFilter("NOT (fare = 0 OR city.Name = 'hello world')", Optional.of("NOT (fare: 0 OR city.Name: \"hello world\")"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testInPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("city.Name IN ('hello world', 'hello world 2')", Optional.of("(city.Name: \"hello world\" OR city.Name: \"hello world 2\")"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testIsNullPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("city.Name IS NULL", Optional.of("NOT city.Name: *"), Optional.empty(), sessionHolder);
        testFilter("city.Name IS NOT NULL", Optional.of("NOT NOT city.Name: *"), Optional.empty(), sessionHolder);
        testFilter("NOT (city.Name IS NULL)", Optional.of("NOT NOT city.Name: *"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testComplexPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("(fare > 0 OR city.Name like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                Optional.of("((fare > 0 OR city.Name: \"b*\"))"),
                Optional.of("(lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)"),
                sessionHolder);
        testFilter("city.Region.Id = 1 AND (fare > 0 OR city.Name NOT like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                Optional.of("((city.Region.Id: 1 AND (fare > 0 OR NOT city.Name: \"b*\")))"),
                Optional.of("lower(city.Region.Name) = 'hello world' OR city.Name IS NULL"),
                sessionHolder);
    }
}
