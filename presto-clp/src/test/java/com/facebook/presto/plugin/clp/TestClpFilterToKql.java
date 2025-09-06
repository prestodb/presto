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

import com.facebook.presto.plugin.clp.optimization.ClpFilterToKqlConverter;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestClpFilterToKql
        extends TestClpQueryBase
{
    @Test
    public void testStringMatchPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Exact match
        testPushDown(sessionHolder, "city.Name = 'hello world'", "city.Name: \"hello world\"", null);
        testPushDown(sessionHolder, "'hello world' = city.Name", "city.Name: \"hello world\"", null);

        // Like predicates that are transformed into substring match
        testPushDown(sessionHolder, "city.Name like 'hello%'", "city.Name: \"hello*\"", null);
        testPushDown(sessionHolder, "city.Name like '%hello'", "city.Name: \"*hello\"", null);

        // Like predicate not pushed down
        testPushDown(sessionHolder, "city.Name like '%hello%'", null, "city.Name like '%hello%'");

        // Like predicates that are kept in the original forms
        testPushDown(sessionHolder, "city.Name like 'hello_'", "city.Name: \"hello?\"", null);
        testPushDown(sessionHolder, "city.Name like '_hello'", "city.Name: \"?hello\"", null);
        testPushDown(sessionHolder, "city.Name like 'hello_w%'", "city.Name: \"hello?w*\"", null);
        testPushDown(sessionHolder, "city.Name like '%hello_w'", "city.Name: \"*hello?w\"", null);
        testPushDown(sessionHolder, "city.Name like 'hello%world'", "city.Name: \"hello*world\"", null);
        testPushDown(sessionHolder, "city.Name like 'hello%wor%ld'", "city.Name: \"hello*wor*ld\"", null);
    }

    @Test
    public void testSubStringPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testPushDown(sessionHolder, "substr(city.Name, 1, 2) = 'he'", "city.Name: \"he*\"", null);
        testPushDown(sessionHolder, "substr(city.Name, 5, 2) = 'he'", "city.Name: \"????he*\"", null);
        testPushDown(sessionHolder, "substr(city.Name, 5) = 'he'", "city.Name: \"????he\"", null);
        testPushDown(sessionHolder, "substr(city.Name, -2) = 'he'", "city.Name: \"*he\"", null);

        // Invalid substring index — not pushed down
        testPushDown(sessionHolder, "substr(city.Name, 1, 5) = 'he'", null, "substr(city.Name, 1, 5) = 'he'");
        testPushDown(sessionHolder, "substr(city.Name, -5) = 'he'", null, "substr(city.Name, -5) = 'he'");
    }

    @Test
    public void testNumericComparisonPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Numeric comparisons
        testPushDown(sessionHolder, "fare > 0", "fare > 0", null);
        testPushDown(sessionHolder, "fare >= 0", "fare >= 0", null);
        testPushDown(sessionHolder, "fare < 0", "fare < 0", null);
        testPushDown(sessionHolder, "fare <= 0", "fare <= 0", null);
        testPushDown(sessionHolder, "fare = 0", "fare: 0", null);
        testPushDown(sessionHolder, "fare != 0", "NOT fare: 0", null);
        testPushDown(sessionHolder, "fare <> 0", "NOT fare: 0", null);
        testPushDown(sessionHolder, "0 < fare", "fare > 0", null);
        testPushDown(sessionHolder, "0 <= fare", "fare >= 0", null);
        testPushDown(sessionHolder, "0 > fare", "fare < 0", null);
        testPushDown(sessionHolder, "0 >= fare", "fare <= 0", null);
        testPushDown(sessionHolder, "0 = fare", "fare: 0", null);
        testPushDown(sessionHolder, "0 != fare", "NOT fare: 0", null);
        testPushDown(sessionHolder, "0 <> fare", "NOT fare: 0", null);
    }

    @Test
    public void testBetweenPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Normal cases
        testPushDown(sessionHolder, "fare BETWEEN 0 AND 5", "fare >= 0 AND fare <= 5", null);
        testPushDown(sessionHolder, "fare BETWEEN 5 AND 0", "fare >= 5 AND fare <= 0", null);

        // No push down for non-constant expressions
        testPushDown(
                sessionHolder,
                "fare BETWEEN (city.Region.Id - 5) AND (city.Region.Id + 5)",
                null,
                "fare BETWEEN (city.Region.Id - 5) AND (city.Region.Id + 5)");

        // If the last two arguments of BETWEEN are not numeric constants, then the CLP connector
        // won't push them down.
        testPushDown(sessionHolder, "city.Name BETWEEN 'a' AND 'b'", null, "city.Name BETWEEN 'a' AND 'b'");
    }

    @Test
    public void testOrPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // OR conditions with partial push down support
        testPushDown(sessionHolder, "fare > 0 OR city.Name like 'b%'", "(fare > 0 OR city.Name: \"b*\")", null);
        testPushDown(
                sessionHolder,
                "lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                null,
                "(lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1)");

        // Multiple ORs
        testPushDown(
                sessionHolder,
                "fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                null,
                "fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1");
        testPushDown(
                sessionHolder,
                "fare > 0 OR city.Name like 'b%' OR city.Region.Id != 1",
                "((fare > 0 OR city.Name: \"b*\") OR NOT city.Region.Id: 1)",
                null);
    }

    @Test
    public void testAndPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // AND conditions with partial/full push down
        testPushDown(sessionHolder, "fare > 0 AND city.Name like 'b%'", "(fare > 0 AND city.Name: \"b*\")", null);

        testPushDown(sessionHolder, "lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1", "(NOT city.Region.Id: 1)", "lower(city.Region.Name) = 'hello world'");

        // Multiple ANDs
        testPushDown(
                sessionHolder,
                "fare > 0 AND city.Name like 'b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                "(((fare > 0 AND city.Name: \"b*\")) AND NOT city.Region.Id: 1)",
                "(lower(city.Region.Name) = 'hello world')");

        testPushDown(
                sessionHolder,
                "fare > 0 AND city.Name like '%b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                "(((fare > 0)) AND NOT city.Region.Id: 1)",
                "city.Name like '%b%' AND lower(city.Region.Name) = 'hello world'");
    }

    @Test
    public void testNotPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // NOT and inequality predicates
        testPushDown(sessionHolder, "city.Region.Name NOT LIKE 'hello%'", "NOT city.Region.Name: \"hello*\"", null);
        testPushDown(sessionHolder, "NOT (city.Region.Name LIKE 'hello%')", "NOT city.Region.Name: \"hello*\"", null);
        testPushDown(sessionHolder, "city.Name != 'hello world'", "NOT city.Name: \"hello world\"", null);
        testPushDown(sessionHolder, "city.Name <> 'hello world'", "NOT city.Name: \"hello world\"", null);
        testPushDown(sessionHolder, "NOT (city.Name = 'hello world')", "NOT city.Name: \"hello world\"", null);
        testPushDown(sessionHolder, "fare != 0", "NOT fare: 0", null);
        testPushDown(sessionHolder, "fare <> 0", "NOT fare: 0", null);
        testPushDown(sessionHolder, "NOT (fare = 0)", "NOT fare: 0", null);

        // Multiple NOTs
        testPushDown(sessionHolder, "NOT (NOT fare = 0)", "NOT NOT fare: 0", null);
        testPushDown(sessionHolder, "NOT (fare = 0 AND city.Name = 'hello world')", "NOT (fare: 0 AND city.Name: \"hello world\")", null);
        testPushDown(sessionHolder, "NOT (fare = 0 OR city.Name = 'hello world')", "NOT (fare: 0 OR city.Name: \"hello world\")", null);
    }

    @Test
    public void testInPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // IN predicate
        testPushDown(sessionHolder, "city.Name IN ('hello world', 'hello world 2')", "(city.Name: \"hello world\" OR city.Name: \"hello world 2\")", null);
    }

    @Test
    public void testIsNullPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // IS NULL / IS NOT NULL predicates
        testPushDown(sessionHolder, "city.Name IS NULL", "NOT city.Name: *", null);
        testPushDown(sessionHolder, "city.Name IS NOT NULL", "NOT NOT city.Name: *", null);
        testPushDown(sessionHolder, "NOT (city.Name IS NULL)", "NOT NOT city.Name: *", null);
    }

    @Test
    public void testComplexPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Complex AND/OR with partial pushdown
        testPushDown(
                sessionHolder,
                "(fare > 0 OR city.Name like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                "((fare > 0 OR city.Name: \"b*\"))",
                "(lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)");

        testPushDown(
                sessionHolder,
                "city.Region.Id = 1 AND (fare > 0 OR city.Name NOT like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                "((city.Region.Id: 1 AND (fare > 0 OR NOT city.Name: \"b*\")))",
                "lower(city.Region.Name) = 'hello world' OR city.Name IS NULL");
    }

    @Test
    public void testMetadataSqlGeneration()
    {
        SessionHolder sessionHolder = new SessionHolder();
        Set<String> testMetadataFilterColumns = ImmutableSet.of("fare");

        // Normal case
        testPushDown(
                sessionHolder,
                "(fare > 0 AND city.Name like 'b%')",
                "(fare > 0 AND city.Name: \"b*\")",
                "(\"fare\" > 0)",
                testMetadataFilterColumns);

        // With BETWEEN
        testPushDown(
                sessionHolder,
                "((fare BETWEEN 0 AND 5) AND city.Name like 'b%')",
                "(fare >= 0 AND fare <= 5 AND city.Name: \"b*\")",
                "(\"fare\" >= 0 AND \"fare\" <= 5)",
                testMetadataFilterColumns);

        // The cases of that the metadata filter column exist but cannot be push down
        testPushDown(
                sessionHolder,
                "(fare > 0 OR city.Name like 'b%')",
                "(fare > 0 OR city.Name: \"b*\")",
                null,
                testMetadataFilterColumns);
        testPushDown(
                sessionHolder,
                "(fare > 0 AND city.Name like 'b%') OR city.Region.Id = 1",
                "((fare > 0 AND city.Name: \"b*\") OR city.Region.Id: 1)",
                null,
                testMetadataFilterColumns);

        // Complicated case
        testPushDown(
                sessionHolder,
                "fare = 0 AND (city.Name like 'b%' OR city.Region.Id = 1)",
                "(fare: 0 AND (city.Name: \"b*\" OR city.Region.Id: 1))",
                "(\"fare\" = 0)",
                testMetadataFilterColumns);
    }

    private void testPushDown(SessionHolder sessionHolder, String sql, String expectedKql, String expectedRemaining)
    {
        ClpExpression clpExpression = tryPushDown(sql, sessionHolder, ImmutableSet.of());
        testFilter(clpExpression, expectedKql, expectedRemaining, sessionHolder);
    }

    private void testPushDown(SessionHolder sessionHolder, String sql, String expectedKql, String expectedMetadataSqlQuery, Set<String> metadataFilterColumns)
    {
        ClpExpression clpExpression = tryPushDown(sql, sessionHolder, metadataFilterColumns);
        testFilter(clpExpression, expectedKql, null, sessionHolder);
        if (expectedMetadataSqlQuery != null) {
            assertTrue(clpExpression.getMetadataSqlQuery().isPresent());
            assertEquals(clpExpression.getMetadataSqlQuery().get(), expectedMetadataSqlQuery);
        }
        else {
            assertFalse(clpExpression.getMetadataSqlQuery().isPresent());
        }
    }

    private ClpExpression tryPushDown(
            String sqlExpression,
            SessionHolder sessionHolder,
            Set<String> metadataFilterColumns)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>(variableToColumnHandleMap);
        return pushDownExpression.accept(
                new ClpFilterToKqlConverter(
                        standardFunctionResolution,
                        functionAndTypeManager,
                        assignments,
                        metadataFilterColumns),
                null);
    }

    private void testFilter(
            ClpExpression clpExpression,
            String expectedKqlExpression,
            String expectedRemainingExpression,
            SessionHolder sessionHolder)
    {
        Optional<String> kqlExpression = clpExpression.getPushDownExpression();
        Optional<RowExpression> remainingExpression = clpExpression.getRemainingExpression();
        if (expectedKqlExpression != null) {
            assertTrue(kqlExpression.isPresent());
            assertEquals(kqlExpression.get(), expectedKqlExpression);
        }
        else {
            assertFalse(kqlExpression.isPresent());
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
