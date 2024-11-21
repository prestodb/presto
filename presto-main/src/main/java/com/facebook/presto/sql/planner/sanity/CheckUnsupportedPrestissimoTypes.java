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
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.IntermediateFormExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.SimplePlanVisitor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.type.IpAddressType.IPADDRESS;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CheckUnsupportedPrestissimoTypes
        implements PlanChecker.Checker
{
    private static final String timestampWithTimeszoneErrorMessage = "Timestamp with Timezone type is not supported in Prestissimo";
    private static final String ipAddressErrorMessage = "IPAddress type is not supported in Prestissimo";
    private FeaturesConfig config;

    public CheckUnsupportedPrestissimoTypes(FeaturesConfig config)
    {
        this.config = requireNonNull(config);
    }

    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        planNode.accept(new Visitor(), null);
    }

    private class Visitor
            extends SimplePlanVisitor<Void>
    {
        private final UnsupportedTypeChecker unsupportedTypeChecker;

        public Visitor()
        {
            this.unsupportedTypeChecker = new UnsupportedTypeChecker();
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            Optional<String> res = node.getOutputVariables().stream().map(x -> getUnsupportedTypeErrorMessage(x.getType())).filter(Objects::nonNull).findFirst().orElse(Optional.empty());
            res.ifPresent(str -> checkState(false, str));
            return super.visitPlan(node, context);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            visitPlan(node, context);
            node.getAggregations().forEach((variable, aggregation) -> {
                aggregation.getCall().accept(unsupportedTypeChecker, null);
                if (aggregation.getFilter().isPresent()) {
                    aggregation.getFilter().get().accept(unsupportedTypeChecker, null);
                }
            });

            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            visitPlan(node, context);
            node.getWindowFunctions().forEach((variable, function) -> {
                function.getFunctionCall().accept(unsupportedTypeChecker, null);
            });

            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            visitPlan(node, context);
            node.getAssignments().getMap().forEach((variable, expression) -> {
                expression.accept(unsupportedTypeChecker, null);
            });

            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            visitPlan(node, context);
            for (List<RowExpression> row : node.getRows()) {
                row.forEach(x -> x.accept(unsupportedTypeChecker, null));
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            visitPlan(node, context);
            node.getPredicate().accept(unsupportedTypeChecker, null);
            return null;
        }
    }

    private class UnsupportedTypeChecker
            extends DefaultRowExpressionTraversalVisitor<Void>
    {
        @Override
        public Void visitConstant(ConstantExpression literal, Void context)
        {
            Optional<String> errorMessage = getUnsupportedTypeErrorMessage(literal.getType());
            checkState(!errorMessage.isPresent(), errorMessage);
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            getUnsupportedTypeErrorMessage(reference.getType()).ifPresent(str -> checkState(false, str));
            return null;
        }

        @Override
        public Void visitInputReference(InputReferenceExpression input, Void context)
        {
            getUnsupportedTypeErrorMessage(input.getType()).ifPresent(str -> checkState(false, str));
            return null;
        }

        @Override
        public Void visitCall(CallExpression call, Void context)
        {
            getUnsupportedTypeErrorMessage(call.getType()).ifPresent(str -> checkState(false, str));
            return super.visitCall(call, context);
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            getUnsupportedTypeErrorMessage(specialForm.getType()).ifPresent(str -> checkState(false, str));
            return super.visitSpecialForm(specialForm, context);
        }

        @Override
        public Void visitIntermediateFormExpression(IntermediateFormExpression expression, Void context)
        {
            getUnsupportedTypeErrorMessage(expression.getType()).ifPresent(str -> checkState(false, str));
            return super.visitIntermediateFormExpression(expression, context);
        }
    }

    private Optional<String> getUnsupportedTypeErrorMessage(Type type)
    {
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE) && config.isDisableTimeStampWithTimeZoneForNative()) {
            return Optional.of(timestampWithTimeszoneErrorMessage);
        }
        if (type.equals(IPADDRESS) && config.isDisableIPAddressForNative()) {
            return Optional.of(ipAddressErrorMessage);
        }
        if (type instanceof ArrayType) {
            return getUnsupportedTypeErrorMessage(((ArrayType) type).getElementType());
        }
        else if (type instanceof MapType) {
            Optional<String> key = getUnsupportedTypeErrorMessage(((MapType) type).getKeyType());
            return key.isPresent() ? key : getUnsupportedTypeErrorMessage(((MapType) type).getValueType());
        }
        else if (type instanceof RowType) {
            return ((RowType) type).getTypeParameters().stream().map(this::getUnsupportedTypeErrorMessage).filter(opt -> opt.isPresent()).findFirst().orElse(Optional.empty());
        }
        return Optional.empty();
    }
}
