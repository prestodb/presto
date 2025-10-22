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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;

public class TestMetadataDeleteOptimizer
        extends BasePlanTest
{
    @Test
    public void testDeleteWithRandFilter()
    {
        // This query shouldn't produce MetadataDeleteNode.
        // It should fail because RAND() requires row-level filtering.
        assertPlanFailedWithException(
                "DELETE FROM orders WHERE rand() < 0.1",
                getQueryRunner().getDefaultSession(),
                "This connector only supports delete where one or more partitions are deleted entirely");
    }

    @Test
    public void testDeleteWithPartitionAndRandFilter()
    {
        // This query shouldn't produce MetadataDeleteNode.
        // Even though orderstatus is a column filter, the RAND() prevents optimization to Metadata Delete.
        assertPlanFailedWithException(
                "DELETE FROM orders WHERE orderstatus = 'F' AND rand() <= 0.1",
                getQueryRunner().getDefaultSession(),
                "This connector only supports delete where one or more partitions are deleted entirely");
    }

    @Test
    public void testDeleteWithPartitionFilter()
    {
        // This query should produce MetadataDeleteNode.
        // orderstatus is a partition column, so this can be optimized to Metadata Delete.
        assertPlan("DELETE FROM orders WHERE orderstatus = 'F'",
                anyTree(
                        node(MetadataDeleteNode.class)));
    }

    @Test
    public void testDeleteWithNonPartitionFilter()
    {
        // This query shouldn't produce MetadataDeleteNode.
        // totalprice isn't a partition column, requires row-level filtering.
        assertPlanFailedWithException(
                "DELETE FROM orders WHERE totalprice > 1000",
                getQueryRunner().getDefaultSession(),
                "This connector only supports delete where one or more partitions are deleted entirely");
    }
}
