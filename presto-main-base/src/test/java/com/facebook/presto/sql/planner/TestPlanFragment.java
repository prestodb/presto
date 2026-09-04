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
package com.facebook.presto.sql.planner;

import com.google.common.base.Strings;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestPlanFragment
{
    private static final int JSON_REPRESENTATION_LENGTH = 4_096;

    @Test
    public void testWithoutJsonRepresentationClearsOnlyTheJsonRepresentation()
    {
        PlanFragment fragment = withJsonRepresentation(Strings.repeat("x", JSON_REPRESENTATION_LENGTH));
        assertTrue(fragment.getJsonRepresentation().isPresent());

        PlanFragment stripped = fragment.withoutJsonRepresentation();

        assertFalse(stripped.getJsonRepresentation().isPresent());
        assertTrue(stripped.getStatsAndCosts().isPresent());
        assertEquals(stripped.getId(), fragment.getId());
        assertEquals(stripped.getVariables(), fragment.getVariables());
        assertSame(stripped.getRoot(), fragment.getRoot());
    }

    @Test
    public void testWithoutJsonRepresentationIsIdentityWhenAlreadyAbsent()
    {
        PlanFragment fragment = createPlanFragment();
        assertFalse(fragment.getJsonRepresentation().isPresent());

        assertSame(fragment.withoutJsonRepresentation(), fragment);
    }

    private static PlanFragment withJsonRepresentation(String jsonRepresentation)
    {
        PlanFragment base = createPlanFragment();
        return new PlanFragment(
                base.getId(),
                base.getRoot(),
                base.getVariables(),
                base.getPartitioning(),
                base.getTableScanSchedulingOrder(),
                base.getPartitioningScheme(),
                base.getOutputOrderingScheme(),
                base.getStageExecutionDescriptor(),
                base.isOutputTableWriterFragment(),
                base.getStatsAndCosts(),
                Optional.of(jsonRepresentation));
    }
}
