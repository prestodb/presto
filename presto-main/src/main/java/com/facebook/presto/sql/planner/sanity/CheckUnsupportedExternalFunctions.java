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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.ExternalCallExpressionChecker;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CheckUnsupportedExternalFunctions
        implements PlanChecker.Checker
{
    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        planNode.accept(new Visitor(metadata.getFunctionAndTypeManager()), null);
    }

    private static class Visitor
            extends SimplePlanVisitor<Void>
    {
        private final ExternalCallExpressionChecker externalCallExpressionChecker;

        Visitor(FunctionAndTypeManager functionAndTypeManager)
        {
            this.externalCallExpressionChecker = new ExternalCallExpressionChecker(requireNonNull(functionAndTypeManager, "functionManager is null"));
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            checkState(!node.getPredicate().accept(externalCallExpressionChecker, null), "Expect FilterNode predicate with external functions be converted to project: %s", node.getPredicate());

            node.getSource().accept(this, context);
            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            checkState(
                    node.getAggregations().values().stream()
                            .noneMatch(
                                    aggregation -> aggregation.getFilter().isPresent() ||
                                            aggregation.getCall().accept(externalCallExpressionChecker, null)),
                    "Expect aggregation to be local");

            node.getSource().accept(this, context);
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            if (node.getFilter().isPresent() && node.getFilter().get().accept(externalCallExpressionChecker, null)) {
                throw new PrestoException(NOT_SUPPORTED, format("External function in join filter is not supported: %s", node.getFilter().get()));
            }

            node.getSources().forEach(child -> child.accept(this, context));
            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Void context)
        {
            throw new IllegalStateException("Do not expect ApplyNode");
        }

        @Override
        public Void visitLateralJoin(LateralJoinNode node, Void context)
        {
            throw new IllegalStateException("Do not expect ApplyNode");
        }
    }
}
