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

import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;

public class PlanNodeUtils
{
    public static boolean isConstantSymbol(PlanNode planNode, Symbol symbol)
    {
        if (planNode instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) planNode;
            Expression expression = projectNode.getAssignments().get(symbol);
            return DependencyExtractor.extractUnique(expression).stream().allMatch(s -> isConstantSymbol(projectNode.getSource(), s));
        }

        if (planNode instanceof ExchangeNode) {
            ExchangeNode exchangeNode = (ExchangeNode) planNode;
            if (exchangeNode.getType() == ExchangeNode.Type.GATHER && exchangeNode.getSources().size() == 1) {
                for (int output = 0; output < exchangeNode.getOutputSymbols().size(); output++) {
                    if (exchangeNode.getOutputSymbols().get(output).equals(symbol)) {
                        return isConstantSymbol(exchangeNode.getSources().get(0), exchangeNode.getInputs().get(0).get(output));
                    }
                }

            }
            // TODO support more exchange types and more sources
        }
        // TODO support more plan nodes
        return false;
    }
}
