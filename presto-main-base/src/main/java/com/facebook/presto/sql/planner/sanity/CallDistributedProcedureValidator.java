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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;

import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;

public final class CallDistributedProcedureValidator
        implements PlanChecker.Checker
{
    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        Optional<PlanNode> callDistributedProcedureNode = searchFrom(planNode)
                .where(node -> node instanceof CallDistributedProcedureNode)
                .findFirst();

        if (!callDistributedProcedureNode.isPresent()) {
            // not a call distributed procedure plan
            return;
        }

        searchFrom(planNode)
                .findAll()
                .forEach(node -> {
                    if (!isAllowedNode(node)) {
                        throw new IllegalStateException("Unexpected " + node.getClass().getSimpleName() + " found in plan; probably connector was not able to handle provided WHERE expression");
                    }
                });
    }

    private boolean isAllowedNode(PlanNode node)
    {
        return node instanceof TableScanNode
                || node instanceof ValuesNode
                || node instanceof ProjectNode
                || node instanceof CallDistributedProcedureNode
                || node instanceof OutputNode
                || node instanceof ExchangeNode
                || node instanceof TableFinishNode;
    }
}
