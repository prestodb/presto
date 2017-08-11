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
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.FieldId;
import com.facebook.presto.sql.analyzer.RelationId;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.type.ListLiteralType;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.operator.scalar.GroupingOperationFunction.BIGINT_GROUPING;
import static com.facebook.presto.operator.scalar.GroupingOperationFunction.INTEGER_GROUPING;
import static com.facebook.presto.operator.scalar.GroupingOperationFunction.MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.resolveFunction;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class GroupingOperationRewriter
{
    private GroupingOperationRewriter() {}

    public static Expression rewriteGroupingOperation(GroupingOperation expression, QuerySpecification queryNode, Analysis analysis, Metadata metadata, Optional<Symbol> groupIdSymbol)
    {
        requireNonNull(queryNode, "node is null");
        requireNonNull(analysis, "analysis is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(groupIdSymbol, "groupIdSymbol is null");

        checkState(queryNode.getGroupBy().isPresent(), "GroupBy node must be present");

        // No GroupIdNode and a GROUPING() operation imply a single grouping, which
        // means that any columns specified as arguments to GROUPING() will be included
        // in the group and none of them will be aggregated over. Hence, re-write the
        // GroupingOperation to a constant literal of 0.
        // See SQL:2011:4.16.2 and SQL:2011:6.9.10.
        if (analysis.getGroupingSets(queryNode).size() == 1) {
            if (shouldUseIntegerReturnType(expression)) {
                return new LongLiteral("0");
            }
            else {
                return new GenericLiteral(StandardTypes.BIGINT, "0");
            }
        }
        else {
            checkState(groupIdSymbol.isPresent(), "groupId symbol is missing");

            Map<NodeRef<Expression>, FieldId> columnReferenceFields = analysis.getColumnReferenceFields();
            RelationId relationId = columnReferenceFields.get(NodeRef.of(expression.getGroupingColumns().get(0))).getRelationId();
            List<Expression> groupingOrdinals = expression.getGroupingColumns().stream()
                    .map(NodeRef::of)
                    .peek(groupingColumn -> checkState(columnReferenceFields.containsKey(groupingColumn), "the grouping column is not in the columnReferencesField map"))
                    .map(columnReferenceFields::get)
                    .map(fieldId -> translateFieldToLongLiteral(fieldId, relationId))
                    .collect(toImmutableList());

            List<List<Expression>> groupingSetOrdinals = analysis.getGroupingSets(queryNode).stream()
                    .map(groupingSet -> groupingSet.stream()
                            .map(NodeRef::of)
                            .filter(columnReferenceFields::containsKey)
                            .map(columnReferenceFields::get)
                            .map(fieldId -> translateFieldToLongLiteral(fieldId, relationId))
                            .collect(toImmutableList()))
                    .collect(toImmutableList());

            List<Expression> newGroupingArguments = ImmutableList.of(
                    groupIdSymbol.get().toSymbolReference(),
                    new Cast(new ArrayConstructor(groupingOrdinals), ListLiteralType.NAME),
                    new Cast(new ArrayConstructor(groupingSetOrdinals.stream().map(ArrayConstructor::new).collect(toImmutableList())), ListLiteralType.NAME));

            FunctionCall rewritten = new FunctionCall(
                    expression.getLocation().get(),
                    shouldUseIntegerReturnType(expression) ? QualifiedName.of(INTEGER_GROUPING) : QualifiedName.of(BIGINT_GROUPING),
                    newGroupingArguments);
            List<TypeSignatureProvider> functionArgumentTypes = Arrays.asList(
                    new TypeSignatureProvider(BIGINT.getTypeSignature()),
                    new TypeSignatureProvider(ListLiteralType.LIST_LITERAL.getTypeSignature()),
                    new TypeSignatureProvider(ListLiteralType.LIST_LITERAL.getTypeSignature()));
            resolveFunction(rewritten, functionArgumentTypes, metadata.getFunctionRegistry());

            return rewritten;
        }
    }

    private static Expression translateFieldToLongLiteral(FieldId fieldId, RelationId requiredOriginRelationId)
    {
        // TODO: this section should be rewritten when support is added for GROUP BY columns to reference an outer scope
        checkState(fieldId.getRelationId().equals(requiredOriginRelationId), "grouping arguments must all come from the same relation");
        return new LongLiteral(Integer.toString(fieldId.getFieldIndex()));
    }

    private static boolean shouldUseIntegerReturnType(GroupingOperation groupingOperation)
    {
        return groupingOperation.getGroupingColumns().size() <= MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER;
    }
}
