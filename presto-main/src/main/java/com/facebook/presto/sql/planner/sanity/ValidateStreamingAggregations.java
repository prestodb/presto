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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.LocalProperties;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.sanity.PlanChecker.Checker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.derivePropertiesRecursively;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Verifies that input of streaming aggregations is grouped on the grouping keys
 */
public class ValidateStreamingAggregations
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
        private final Session sesstion;
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final TypeProvider types;
        private final WarningCollector warningCollector;

        private Visitor(Session sesstion, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
        {
            this.sesstion = sesstion;
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
        public Void visitAggregation(AggregationNode node, Void context)
        {
            if (node.getPreGroupedVariables().isEmpty()) {
                return null;
            }

            StreamProperties properties = derivePropertiesRecursively(node.getSource(), metadata, sesstion, types, sqlParser);

            List<LocalProperty<VariableReferenceExpression>> desiredProperties = ImmutableList.of(new GroupingProperty<>(node.getPreGroupedVariables()));
            Iterator<Optional<LocalProperty<VariableReferenceExpression>>> matchIterator = LocalProperties.match(properties.getLocalProperties(), desiredProperties).iterator();
            Optional<LocalProperty<VariableReferenceExpression>> unsatisfiedRequirement = Iterators.getOnlyElement(matchIterator);
            checkArgument(!unsatisfiedRequirement.isPresent(), "Streaming aggregation with input not grouped on the grouping keys");
            return null;
        }
    }
}
