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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.IntermediateFormExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.WindowNode;

import java.util.List;

import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.google.common.base.Preconditions.checkState;

public class CheckNoTimestampWithTimezoneType
        implements PlanChecker.Checker
{
    private static final String errorMessage = "Timestamp with Timezone type is not supported in Prestissimo";

    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        planNode.accept(new Visitor(), null);
    }

    private static class Visitor
            extends SimplePlanVisitor<Void>
    {
        private final NoTimeStampWithTimeZoneTypeChecker noTimeStampWithTimeZoneTypeChecker;

        public Visitor()
        {
            this.noTimeStampWithTimeZoneTypeChecker = new NoTimeStampWithTimeZoneTypeChecker();
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            checkState(node.getOutputVariables().stream().noneMatch(x -> hasTimestampWithTimezoneType(x.getType())), errorMessage);
            return super.visitPlan(node, context);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            visitPlan(node, context);
            node.getAggregations().forEach((variable, aggregation) -> {
                aggregation.getCall().accept(noTimeStampWithTimeZoneTypeChecker, null);
                if (aggregation.getFilter().isPresent()) {
                    aggregation.getFilter().get().accept(noTimeStampWithTimeZoneTypeChecker, null);
                }
            });

            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            visitPlan(node, context);
            node.getWindowFunctions().forEach((variable, function) -> {
                function.getFunctionCall().accept(noTimeStampWithTimeZoneTypeChecker, null);
            });

            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            visitPlan(node, context);
            node.getAssignments().getMap().forEach((variable, expression) -> {
                expression.accept(noTimeStampWithTimeZoneTypeChecker, null);
            });

            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            visitPlan(node, context);
            for (List<RowExpression> row : node.getRows()) {
                row.forEach(x -> x.accept(noTimeStampWithTimeZoneTypeChecker, null));
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            visitPlan(node, context);
            node.getPredicate().accept(noTimeStampWithTimeZoneTypeChecker, null);
            return null;
        }
    }

    private static class NoTimeStampWithTimeZoneTypeChecker
            extends DefaultRowExpressionTraversalVisitor<Void>
    {
        @Override
        public Void visitConstant(ConstantExpression literal, Void context)
        {
            checkState(!hasTimestampWithTimezoneType(literal.getType()), errorMessage);
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            checkState(!hasTimestampWithTimezoneType(reference.getType()), errorMessage);
            return null;
        }

        @Override
        public Void visitInputReference(InputReferenceExpression input, Void context)
        {
            checkState(!hasTimestampWithTimezoneType(input.getType()), errorMessage);
            return null;
        }

        @Override
        public Void visitCall(CallExpression call, Void context)
        {
            checkState(!hasTimestampWithTimezoneType(call.getType()), errorMessage);
            return super.visitCall(call, context);
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            checkState(!hasTimestampWithTimezoneType(specialForm.getType()), errorMessage);
            return super.visitSpecialForm(specialForm, context);
        }

        @Override
        public Void visitIntermediateFormExpression(IntermediateFormExpression expression, Void context)
        {
            checkState(!hasTimestampWithTimezoneType(expression.getType()), errorMessage);
            return super.visitIntermediateFormExpression(expression, context);
        }
    }

    private static boolean hasTimestampWithTimezoneType(Type type)
    {
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return true;
        }
        if (type instanceof ArrayType) {
            return hasTimestampWithTimezoneType(((ArrayType) type).getElementType());
        }
        else if (type instanceof MapType) {
            return hasTimestampWithTimezoneType(((MapType) type).getKeyType()) || hasTimestampWithTimezoneType(((MapType) type).getValueType());
        }
        else if (type instanceof RowType) {
            return ((RowType) type).getTypeParameters().stream().anyMatch(CheckNoTimestampWithTimezoneType::hasTimestampWithTimezoneType);
        }
        return false;
    }
}
