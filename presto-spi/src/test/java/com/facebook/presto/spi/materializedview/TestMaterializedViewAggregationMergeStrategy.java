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
package com.facebook.presto.spi.materializedview;

import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestMaterializedViewAggregationMergeStrategy
{
    @Test
    public void testCanonicalizesFunctionNames()
    {
        MaterializedViewAggregationMergeStrategy strategy = new MaterializedViewAggregationMergeStrategy("COUNT", Optional.of("SUM"), false);

        assertEquals(strategy.getSourceFunctionName(), "count");
        assertEquals(strategy.getMergeFunctionName(), Optional.of("sum"));
        assertFalse(strategy.isDuplicateInsensitive());
    }

    @Test
    public void testDuplicateInsensitiveStrategyWithoutMergeFunction()
    {
        MaterializedViewAggregationMergeStrategy strategy = new MaterializedViewAggregationMergeStrategy("arbitrary", Optional.empty(), true);

        assertEquals(strategy.getSourceFunctionName(), "arbitrary");
        assertFalse(strategy.getMergeFunctionName().isPresent());
        assertTrue(strategy.isDuplicateInsensitive());
    }

    @Test
    public void testRejectsEmptyFunctionName()
    {
        expectThrows(IllegalArgumentException.class, () -> new MaterializedViewAggregationMergeStrategy("", Optional.of("sum"), false));
    }
}
