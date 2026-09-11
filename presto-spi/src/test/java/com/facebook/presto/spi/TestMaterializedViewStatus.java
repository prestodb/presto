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
package com.facebook.presto.spi;

import com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.FULLY_MATERIALIZED;
import static org.testng.Assert.assertTrue;

public class TestMaterializedViewStatus
{
    @Test
    public void testExistingConstructorsLeaveRowLevelStateEmpty()
    {
        MaterializedViewStatus status = new MaterializedViewStatus(FULLY_MATERIALIZED);

        assertTrue(status.getRecordedBaseTableHandles().isEmpty());
        assertTrue(status.getChangedRowsPredicates().isEmpty());
    }

    @Test
    public void testRowLevelStateIsCopied()
    {
        SchemaTableName baseTable = new SchemaTableName("schema", "base");
        Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates = new HashMap<>();
        changedRowsPredicates.put(baseTable, ChangedRowsPredicate.empty());

        MaterializedViewStatus status = new MaterializedViewStatus(
                MaterializedViewState.PARTIALLY_MATERIALIZED,
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableMap.of(),
                changedRowsPredicates);
        changedRowsPredicates.clear();

        assertTrue(status.getChangedRowsPredicates().containsKey(baseTable));
        assertTrue(status.getChangedRowsPredicates().get(baseTable).getDataDisjuncts().isEmpty());
    }
}
