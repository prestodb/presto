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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isRewriteRowConstructorInToDisjunction;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites predicates of the form:
 * <pre>
 *   ROW(partition_key1, partition_key2) IN (ROW('a', 1), ROW('b', 2), ...)
 * </pre>
 * into:
 * <pre>
 *   (partition_key1 = 'a' AND partition_key2 = 1)
 *   OR (partition_key1 = 'b' AND partition_key2 = 2)
 *   OR ...
 * </pre>
 *
 * This transformation fires when ANY field of the left-side ROW constructor
 * is a partition key column of the underlying table. The rewrite enables
 * {@code PickTableLayout} to extract per-column domains for partition pruning,
 * which is impossible when the predicate uses ROW-level IN comparisons.
 * Fields that are not partition keys still benefit from the rewrite since the
 * domain translator can extract their constraints from the flattened AND chain.
 */
public class RewriteRowConstructorInToDisjunction
        implements Rule<FilterNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
            tableScan().capturedAs(TABLE_SCAN)));
    private static final String PARTITIONED_BY_PROPERTY = "partitioned_by";

    private final Metadata metadata;
    private final FunctionResolution functionResolution;

    public RewriteRowConstructorInToDisjunction(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        if (!isRewriteRowConstructorInToDisjunction(context.getSession())) {
            return Result.empty();
        }

        TableScanNode tableScan = captures.get(TABLE_SCAN);
        Set<VariableReferenceExpression> partitionVars = resolvePartitionVariables(
                context.getSession(), tableScan);

        if (partitionVars.isEmpty()) {
            return Result.empty();
        }

        RowExpression predicate = filterNode.getPredicate();
        RowExpression rewritten = rewritePredicate(predicate, partitionVars);

        if (predicate.equals(rewritten)) {
            return Result.empty();
        }

        return Result.ofPlanNode(new FilterNode(
                filterNode.getSourceLocation(),
                filterNode.getId(),
                filterNode.getSource(),
                rewritten));
    }

    private Set<VariableReferenceExpression> resolvePartitionVariables(
            com.facebook.presto.Session session,
            TableScanNode tableScan)
    {
        TableHandle tableHandle = tableScan.getTable();
        ConnectorTableMetadata tableMetadata;
        try {
            tableMetadata = metadata.getTableMetadata(session, tableHandle).getMetadata();
        }
        catch (RuntimeException e) {
            return ImmutableSet.of();
        }

        Object partitionedByObj = tableMetadata.getProperties().get(PARTITIONED_BY_PROPERTY);
        if (!(partitionedByObj instanceof List)) {
            return ImmutableSet.of();
        }

        @SuppressWarnings("unchecked")
        List<String> partitionColumnNames = (List<String>) partitionedByObj;
        if (partitionColumnNames.isEmpty()) {
            return ImmutableSet.of();
        }

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        Set<ColumnHandle> partitionHandles = new HashSet<>();
        for (String name : partitionColumnNames) {
            ColumnHandle handle = columnHandles.get(name);
            if (handle != null) {
                partitionHandles.add(handle);
            }
        }

        ImmutableSet.Builder<VariableReferenceExpression> result = ImmutableSet.builder();
        for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : tableScan.getAssignments().entrySet()) {
            if (partitionHandles.contains(entry.getValue())) {
                result.add(entry.getKey());
            }
        }
        return result.build();
    }

    /**
     * Walks the predicate tree looking for rewritable ROW IN expressions.
     * Handles AND conjuncts at the top level and rewrites each eligible IN independently.
     */
    private RowExpression rewritePredicate(RowExpression predicate, Set<VariableReferenceExpression> partitionVars)
    {
        if (predicate instanceof SpecialFormExpression && ((SpecialFormExpression) predicate).getForm() == IN) {
            RowExpression rewritten = tryRewriteRowIn((SpecialFormExpression) predicate, partitionVars);
            if (rewritten != null) {
                return rewritten;
            }
            return predicate;
        }

        List<RowExpression> conjuncts = extractConjuncts(predicate);
        if (conjuncts.size() <= 1) {
            return predicate;
        }

        boolean anyChanged = false;
        ImmutableList.Builder<RowExpression> newConjuncts = ImmutableList.builder();
        for (RowExpression conjunct : conjuncts) {
            RowExpression rewritten = rewritePredicate(conjunct, partitionVars);
            if (!rewritten.equals(conjunct)) {
                anyChanged = true;
            }
            newConjuncts.add(rewritten);
        }
        if (anyChanged) {
            return and(newConjuncts.build());
        }
        return predicate;
    }

    /**
     * Attempts to rewrite a single SpecialFormExpression(IN, ...) where the first argument
     * is a ROW_CONSTRUCTOR of partition key variables and all candidates are ROW_CONSTRUCTORs
     * of matching arity.
     *
     * Returns the rewritten expression, or null if the pattern does not match.
     */
    private RowExpression tryRewriteRowIn(SpecialFormExpression inExpr, Set<VariableReferenceExpression> partitionVars)
    {
        List<RowExpression> args = inExpr.getArguments();
        if (args.size() < 2) {
            return null;
        }

        RowExpression target = args.get(0);
        if (!(target instanceof SpecialFormExpression)) {
            return null;
        }

        SpecialFormExpression targetRow = (SpecialFormExpression) target;
        if (targetRow.getForm() != ROW_CONSTRUCTOR) {
            return null;
        }

        List<RowExpression> rowFields = targetRow.getArguments();
        if (rowFields.isEmpty()) {
            return null;
        }

        // All fields must be VariableReferenceExpressions, and at least one must be a partition key
        List<VariableReferenceExpression> fieldVars = new ArrayList<>(rowFields.size());
        boolean hasPartitionKey = false;
        for (RowExpression field : rowFields) {
            if (!(field instanceof VariableReferenceExpression)) {
                return null;
            }
            VariableReferenceExpression varRef = (VariableReferenceExpression) field;
            if (partitionVars.contains(varRef)) {
                hasPartitionKey = true;
            }
            fieldVars.add(varRef);
        }
        if (!hasPartitionKey) {
            return null;
        }

        // All candidate values must be ROW_CONSTRUCTORs with matching arity
        int arity = rowFields.size();
        List<SpecialFormExpression> candidateRows = new ArrayList<>(args.size() - 1);
        for (int i = 1; i < args.size(); i++) {
            if (!(args.get(i) instanceof SpecialFormExpression)) {
                return null;
            }
            SpecialFormExpression candidate = (SpecialFormExpression) args.get(i);
            if (candidate.getForm() != ROW_CONSTRUCTOR || candidate.getArguments().size() != arity) {
                return null;
            }
            candidateRows.add(candidate);
        }

        // Build: (pk1 = v1_1 AND pk2 = v1_2) OR (pk1 = v2_1 AND pk2 = v2_2) OR ...
        ImmutableList.Builder<RowExpression> disjuncts = ImmutableList.builder();
        for (SpecialFormExpression candidate : candidateRows) {
            ImmutableList.Builder<RowExpression> conjuncts = ImmutableList.builder();
            for (int fieldIdx = 0; fieldIdx < arity; fieldIdx++) {
                VariableReferenceExpression leftVar = fieldVars.get(fieldIdx);
                RowExpression rightVal = candidate.getArguments().get(fieldIdx);

                conjuncts.add(new CallExpression(
                        EQUAL.name(),
                        functionResolution.comparisonFunction(EQUAL, leftVar.getType(), rightVal.getType()),
                        BOOLEAN,
                        ImmutableList.of(leftVar, rightVal)));
            }
            disjuncts.add(and(conjuncts.build()));
        }

        return or(disjuncts.build());
    }
}
