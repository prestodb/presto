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
package com.facebook.presto.hive.rule;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveTableLayoutHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class HiveAddRequestedColumnsToLayout
        implements ConnectorPlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(maxSubplan, "maxSubplan is null");
        return rewriteWith(new Rewriter(), maxSubplan);
    }

    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        public Rewriter() {}

        @Override
        public PlanNode visitTableScan(TableScanNode tableScan, RewriteContext<Void> context)
        {
            Optional<ConnectorTableLayoutHandle> layout = tableScan.getTable().getLayout();
            if (!layout.isPresent()) {
                return tableScan;
            }

            HiveTableLayoutHandle hiveLayout = (HiveTableLayoutHandle) layout.get();
            Optional<Set<HiveColumnHandle>> requestedColumns = Optional.of(tableScan.getOutputVariables().stream().map(output -> (HiveColumnHandle) tableScan.getAssignments().get(output)).collect(toImmutableSet()));
            HiveTableLayoutHandle hiveLayoutWithDesiredColumns = hiveLayout.builder().setRequestedColumns(requestedColumns).build();

            return new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    new TableHandle(
                            tableScan.getTable().getConnectorId(),
                            tableScan.getTable().getConnectorHandle(),
                            tableScan.getTable().getTransaction(),
                            Optional.of(hiveLayoutWithDesiredColumns)),
                    tableScan.getOutputVariables(),
                    tableScan.getAssignments(),
                    tableScan.getTableConstraints(),
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint());
        }
    }
}
