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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.RemoteFunctionChecker;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.glassfish.jersey.internal.guava.Preconditions.checkState;

public class VerifyProjectionLocalityChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        planNode.accept(new Visitor(metadata.getFunctionManager()), null);
    }

    private static class Visitor
            extends SimplePlanVisitor<Void>
    {
        private final RemoteFunctionChecker remoteFunctionChecker;

        Visitor(FunctionManager functionManager)
        {
            this.remoteFunctionChecker = new RemoteFunctionChecker(requireNonNull(functionManager, "functionManager is null"));
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            switch (node.getLocality()) {
                case LOCAL:
                    checkState(node.getAssignments().getExpressions().stream().noneMatch(expression -> expression.accept(remoteFunctionChecker, null)), format("ProjectNode with LOCAL locality has remote functions. Assignments are: %s", node.getAssignments()));
                    break;
                case REMOTE:
                    node.getAssignments().getExpressions().forEach(expression -> {
                        checkState(expression instanceof VariableReferenceExpression || expression instanceof CallExpression, format("Expect VariableReferenceExpression or CallExpression, got %s", expression.getClass().getName()));
                        if (expression instanceof CallExpression) {
                            checkState(expression.accept(remoteFunctionChecker, null), format("Expect expression %s to be a remote function", expression));
                        }
                    });
                    break;
                case UNKNOWN:
                    throw new IllegalStateException("ProjectNode should have locality LOCAL or REMOTE");
            }
            return null;
        }
    }
}
