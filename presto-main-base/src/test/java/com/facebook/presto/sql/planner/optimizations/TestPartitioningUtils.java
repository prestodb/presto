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

import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.PartitioningUtils.isPartitionedOn;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPartitioningUtils
{
    @Test
    public void testIsPartitionedOnEmptyArgumentsSingleDistribution()
    {
        Partitioning partitioning = Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of());
        assertTrue(isPartitionedOn(partitioning, ImmutableList.of(), ImmutableSet.of()));
    }

    @Test
    public void testIsPartitionedOnEmptyArgumentsCoordinatorDistribution()
    {
        Partitioning partitioning = Partitioning.create(COORDINATOR_DISTRIBUTION, ImmutableList.of());
        assertTrue(isPartitionedOn(partitioning, ImmutableList.of(), ImmutableSet.of()));
    }

    @Test
    public void testIsPartitionedOnEmptyArgumentsHashDistribution()
    {
        Partitioning partitioning = Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of());
        assertFalse(isPartitionedOn(partitioning, ImmutableList.of(), ImmutableSet.of()));
    }

    @Test
    public void testIsPartitionedOnEmptyArgumentsBroadcastDistribution()
    {
        Partitioning partitioning = Partitioning.create(FIXED_BROADCAST_DISTRIBUTION, ImmutableList.of());
        assertFalse(isPartitionedOn(partitioning, ImmutableList.of(), ImmutableSet.of()));
    }

    @Test
    public void testIsPartitionedOnEmptyArgumentsArbitraryDistribution()
    {
        Partitioning partitioning = Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of());
        assertFalse(isPartitionedOn(partitioning, ImmutableList.of(), ImmutableSet.of()));
    }

    @Test
    public void testIsPartitionedOnEmptyArgumentsSourceDistribution()
    {
        Partitioning partitioning = Partitioning.create(SOURCE_DISTRIBUTION, ImmutableList.of());
        assertFalse(isPartitionedOn(partitioning, ImmutableList.of(), ImmutableSet.of()));
    }

    @Test
    public void testIsPartitionedOnEmptyArgumentsScaledWriterDistribution()
    {
        Partitioning partitioning = Partitioning.create(SCALED_WRITER_DISTRIBUTION, ImmutableList.of());
        assertFalse(isPartitionedOn(partitioning, ImmutableList.of(), ImmutableSet.of()));
    }

    @Test
    public void testIsPartitionedOnWithMatchingColumns()
    {
        VariableReferenceExpression column = new VariableReferenceExpression(Optional.empty(), "col", BIGINT);
        Partitioning partitioning = Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(column));
        assertTrue(isPartitionedOn(partitioning, ImmutableList.of(column), ImmutableSet.of()));
    }

    @Test
    public void testIsPartitionedOnWithNonMatchingColumns()
    {
        VariableReferenceExpression column1 = new VariableReferenceExpression(Optional.empty(), "col1", BIGINT);
        VariableReferenceExpression column2 = new VariableReferenceExpression(Optional.empty(), "col2", BIGINT);
        Partitioning partitioning = Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(column1));
        assertFalse(isPartitionedOn(partitioning, ImmutableList.of(column2), ImmutableSet.of()));
    }

    @Test
    public void testIsPartitionedOnWithKnownConstants()
    {
        VariableReferenceExpression column = new VariableReferenceExpression(Optional.empty(), "col", BIGINT);
        Partitioning partitioning = Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(column));
        assertTrue(isPartitionedOn(partitioning, ImmutableList.of(), ImmutableSet.of(column)));
    }

    @Test
    public void testIsPartitionedOnEmptyArgumentsSingleDistributionWithColumns()
    {
        VariableReferenceExpression column = new VariableReferenceExpression(Optional.empty(), "col", BIGINT);
        Partitioning partitioning = Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of());
        assertTrue(isPartitionedOn(partitioning, ImmutableList.of(column), ImmutableSet.of()));
    }

    @Test
    public void testIsPartitionedOnEmptyArgumentsCoordinatorDistributionWithColumns()
    {
        VariableReferenceExpression column = new VariableReferenceExpression(Optional.empty(), "col", BIGINT);
        Partitioning partitioning = Partitioning.create(COORDINATOR_DISTRIBUTION, ImmutableList.of());
        assertTrue(isPartitionedOn(partitioning, ImmutableList.of(column), ImmutableSet.of()));
    }
}
