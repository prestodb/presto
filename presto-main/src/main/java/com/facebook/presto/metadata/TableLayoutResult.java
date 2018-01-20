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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TableLayoutResult
{
    private final TableLayout layout;
    private final TupleDomain<ColumnHandle> unenforcedConstraint;

    public TableLayoutResult(TableLayout layout, TupleDomain<ColumnHandle> unenforcedConstraint)
    {
        this.layout = layout;
        this.unenforcedConstraint = unenforcedConstraint;
    }

    public TableLayout getLayout()
    {
        return layout;
    }

    public TupleDomain<ColumnHandle> getUnenforcedConstraint()
    {
        return unenforcedConstraint;
    }

    public boolean hasAllOutputs(TableScanNode node)
    {
        if (!layout.getColumns().isPresent()) {
            return true;
        }
        Set<ColumnHandle> columns = ImmutableSet.copyOf(layout.getColumns().get());
        List<ColumnHandle> nodeColumnHandles = node.getOutputSymbols().stream()
                .map(node.getAssignments()::get)
                .collect(toImmutableList());

        return columns.containsAll(nodeColumnHandles);
    }
}
