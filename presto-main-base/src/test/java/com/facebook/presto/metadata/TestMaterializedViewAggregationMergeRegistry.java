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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.materializedview.MaterializedViewAggregationMergeStrategy;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestMaterializedViewAggregationMergeRegistry
{
    @Test
    public void testBuiltInStrategies()
    {
        MaterializedViewAggregationMergeRegistry registry = new MaterializedViewAggregationMergeRegistry();

        assertEquals(registry.getMergeFunction("count"), Optional.of("sum"));
        assertEquals(registry.getMergeFunction("SUM"), Optional.of("sum"));
        assertTrue(registry.isDuplicateInsensitive("min"));
        assertTrue(registry.isDuplicateInsensitive("any_value"));
        assertFalse(registry.getMergeFunction("arbitrary").isPresent());
        assertFalse(registry.getStrategy("avg_partial").isPresent());
    }

    @Test
    public void testPluginStrategy()
    {
        MaterializedViewAggregationMergeRegistry registry = new MaterializedViewAggregationMergeRegistry();
        registry.addStrategies(ImmutableList.of(new MaterializedViewAggregationMergeStrategy("avg_partial", Optional.of("avg_merge"), false)));

        assertEquals(registry.getMergeFunction("avg_partial"), Optional.of("avg_merge"));
    }

    @Test
    public void testRejectsDuplicateSourceFunction()
    {
        MaterializedViewAggregationMergeRegistry registry = new MaterializedViewAggregationMergeRegistry();

        expectThrows(IllegalArgumentException.class, () -> registry.addStrategies(
                ImmutableList.of(new MaterializedViewAggregationMergeStrategy("count", Optional.of("sum"), false))));
    }

    @Test
    public void testRegistrationIsAtomic()
    {
        MaterializedViewAggregationMergeRegistry registry = new MaterializedViewAggregationMergeRegistry();

        expectThrows(IllegalArgumentException.class, () -> registry.addStrategies(ImmutableList.of(
                new MaterializedViewAggregationMergeStrategy("avg_partial", Optional.of("avg_merge"), false),
                new MaterializedViewAggregationMergeStrategy("count", Optional.of("sum"), false))));
        assertFalse(registry.getStrategy("avg_partial").isPresent());
    }
}
