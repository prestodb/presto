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

import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.AbstractMockMetadata.dummyMetadata;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;

public class TestCardinalityExtractorPlanVisitor
{
    @Test
    public void testLimitOnTopOfValues()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), dummyMetadata());

        assertEquals(
                extractCardinality(planBuilder.limit(3, planBuilder.values(emptyList(), ImmutableList.of(emptyList())))),
                Range.singleton(1L));

        assertEquals(
                extractCardinality(planBuilder.limit(3, planBuilder.values(emptyList(), ImmutableList.of(emptyList(), emptyList(), emptyList(), emptyList())))),
                Range.singleton(3L));
    }
}
