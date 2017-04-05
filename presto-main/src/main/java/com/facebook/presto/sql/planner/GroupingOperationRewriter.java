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
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.type.ListLiteralType;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.scalar.GroupingOperationFunction.GROUPING;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.resolveFunction;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
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
        boolean isSimpleGroupByPresent = queryNode.getGroupBy().get().getGroupingElements().stream().anyMatch(SimpleGroupBy.class::isInstance);
        if (isSimpleGroupByPresent) {
            return new LongLiteral("0");
        }
        else {
            checkState(groupIdSymbol.isPresent(), "groupId symbol is missing");

            List<Expression> columnReferences = ImmutableList.copyOf(analysis.getColumnReferences());
            List<Expression> groupingOrdinals = expression.getGroupingColumns().stream()
                    .map(columnReferences::indexOf)
                    .map(columnOrdinal -> new LongLiteral(Integer.toString(columnOrdinal)))
                    .collect(toImmutableList());

            List<List<Expression>> groupingSetOrdinals;
            ImmutableList.Builder<List<Expression>> groupingSetOrdinalsBuilder = ImmutableList.builder();
            for (List<Expression> groupingSet : analysis.getGroupingSets(queryNode)) {
                groupingSetOrdinalsBuilder.add(groupingSet.stream()
                        .map(columnReferences::indexOf)
                        .map(columnOrdinal -> new LongLiteral(Integer.toString(columnOrdinal)))
                        .collect(toImmutableList()));
            }
            groupingSetOrdinals = groupingSetOrdinalsBuilder.build();

            Expression firstGroupingArgument = groupIdSymbol.get().toSymbolReference();
            List<Expression> newGroupingArguments = ImmutableList.of(
                    firstGroupingArgument,
                    new Cast(new ArrayConstructor(groupingOrdinals), ListLiteralType.NAME),
                    new Cast(new ArrayConstructor(groupingSetOrdinals.stream().map(ArrayConstructor::new).collect(toImmutableList())), ListLiteralType.NAME)
            );

            FunctionCall rewritten = new FunctionCall(expression.getLocation().get(), QualifiedName.of(GROUPING), newGroupingArguments);
            List<TypeSignatureProvider> functionTypes = Arrays.asList(
                    new TypeSignatureProvider(BIGINT.getTypeSignature()),
                    new TypeSignatureProvider(ListLiteralType.LIST_LITERAL.getTypeSignature()),
                    new TypeSignatureProvider(ListLiteralType.LIST_LITERAL.getTypeSignature())
            );
            resolveFunction(rewritten, functionTypes, metadata.getFunctionRegistry());

            return rewritten;
        }
    }
}
