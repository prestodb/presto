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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.sanity.PlanChecker.Checker;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.SystemSessionProperties.isJoinSpillingEnabled;
import static com.facebook.presto.SystemSessionProperties.isSpillEnabled;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.defaultParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.exactlyPartitionedOn;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.fixedParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.singleStream;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.derivePropertiesRecursively;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ValidateStreamingJoins
        implements Checker
{
    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        planNode.accept(new Visitor(session, metadata, sqlParser, types, warningCollector), null);
    }

    private static final class Visitor
            extends InternalPlanVisitor<Void, Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final TypeProvider types;
        private final WarningCollector warningCollector;

        private Visitor(Session sesstion, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
        {
            this.session = sesstion;
            this.metadata = metadata;
            this.sqlParser = sqlParser;
            this.types = types;
            this.warningCollector = warningCollector;
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            node.getSources().forEach(source -> source.accept(this, context));
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            // Validate the streaming property of the join node is satisfied when no RemoteSourceNode is involved.
            if (!searchFrom(node).where(RemoteSourceNode.class::isInstance).matches()) {
                List<VariableReferenceExpression> buildJoinVariables = node.getCriteria().stream()
                        .map(JoinNode.EquiJoinClause::getRight)
                        .collect(toImmutableList());
                StreamPreferredProperties requiredBuildProperty;
                if (getTaskConcurrency(session) > 1) {
                    requiredBuildProperty = exactlyPartitionedOn(buildJoinVariables);
                }
                else {
                    requiredBuildProperty = singleStream();
                }
                StreamProperties buildProperties = derivePropertiesRecursively(node.getRight(), metadata, session, types, sqlParser);
                checkArgument(requiredBuildProperty.isSatisfiedBy(buildProperties), "Build side needs an additional local exchange for join: %s", node.getId());

                StreamPreferredProperties requiredProbeProperty;
                if (isSpillEnabled(session) && isJoinSpillingEnabled(session)) {
                    requiredProbeProperty = fixedParallelism();
                }
                else {
                    requiredProbeProperty = defaultParallelism(session);
                }
                StreamProperties probeProperties = derivePropertiesRecursively(node.getLeft(), metadata, session, types, sqlParser);
                checkArgument(requiredProbeProperty.isSatisfiedBy(probeProperties), "Probe side needs an additional local exchange for join: %s", node.getId());
            }
            return null;
        }
    }
}
