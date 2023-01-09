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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.iterative.rule.SimplifyCardinalityMapRewriter.rewrite;
import static org.testng.Assert.assertEquals;

public class TestSimplifyCardinalityMap
        extends BaseRuleTest
{
    @Test
    public void testRewriteMapValuesCardinality()
    {
        assertRewritten("cardinality(map_values(m))", "cardinality(m)");
    }

    @Test
    public void testRewriteMapValuesMixedCasesCardinality()
    {
        assertRewritten("CaRDinality(map_values(m))", "cardinaLITY(m)");
    }

    @Test
    public void testNoRewriteMapValuesCardinality()
    {
        assertRewritten("cardinality(map(ARRAY[1,3], ARRAY[2,4]))", "cardinality(map(ARRAY[1,3], ARRAY[2,4]))");
    }

    @Test
    public void testNestedRewriteMapValuesCardinality()
    {
        assertRewritten(
                "cardinality(map(ARRAY[cardinality(map_values(m_1)),3], ARRAY[2,cardinality(map_values(m_2))]))",
                "cardinality(map(ARRAY[cardinality(m_1),3], ARRAY[2,cardinality(m_2)]))");
    }

    @Test
    public void testNestedRewriteMapKeysCardinality()
    {
        assertRewritten(
                "cardinality(map(ARRAY[cardinality(map_keys(m_1)),3], ARRAY[2,cardinality(map_keys(m_2))]))",
                "cardinality(map(ARRAY[cardinality(m_1),3], ARRAY[2,cardinality(m_2)]))");
    }

    @Test
    public void testAnotherNestedRewriteMapValuesCardinality()
    {
        assertRewritten(
                "cardinality(map(ARRAY[cardinality(map_values(map(ARRAY[1,3], ARRAY[2,4]))),3], ARRAY[2,cardinality(map_values(m_2))]))",
                "cardinality(map(ARRAY[cardinality(map(ARRAY[1,3], ARRAY[2,4])),3], ARRAY[2,cardinality(m_2)]))");
    }

    private static void assertRewritten(String from, String to)
    {
        assertEquals(rewrite(PlanBuilder.expression(from)), PlanBuilder.expression(to));
    }
}
