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
package io.prestosql.tests;

import io.prestosql.tests.tpch.TpchQueryRunnerBuilder;

public class TestOptimizeMixedDistinctAggregations
        extends AbstractTestAggregations
{
    public TestOptimizeMixedDistinctAggregations()
    {
        super(() -> TpchQueryRunnerBuilder.builder()
                .setSingleCoordinatorProperty("optimizer.optimize-mixed-distinct-aggregations", "true")
                .build());
    }

    @Override
    public void testCountDistinct()
    {
        assertQuery("SELECT COUNT(DISTINCT custkey + 1) FROM orders", "SELECT COUNT(*) FROM (SELECT DISTINCT custkey + 1 FROM orders) t");
        assertQuery("SELECT COUNT(DISTINCT linenumber), COUNT(*) from lineitem where linenumber < 0");
    }

    // TODO add dedicated test cases and remove `extends AbstractTestAggregation`
}
