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

import com.facebook.presto.sql.analyzer.FieldId;
import com.facebook.presto.sql.analyzer.RelationId;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SubscriptExpression;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class GroupingOperationRewriter
{
    private GroupingOperationRewriter() {}

    public static Expression rewriteGroupingOperation(GroupingOperation expression, List<List<Expression>> groupingSets, Map<NodeRef<Expression>, FieldId> columnReferenceFields, Optional<Symbol> groupIdSymbol)
    {
        requireNonNull(groupIdSymbol, "groupIdSymbol is null");

        // No GroupIdNode and a GROUPING() operation imply a single grouping, which
        // means that any columns specified as arguments to GROUPING() will be included
        // in the group and none of them will be aggregated over. Hence, re-write the
        // GroupingOperation to a constant literal of 0.
        // See SQL:2011:4.16.2 and SQL:2011:6.9.10.
        if (groupingSets.size() == 1) {
            return new LongLiteral("0");
        }
        else {
            checkState(groupIdSymbol.isPresent(), "groupId symbol is missing");

            RelationId relationId = columnReferenceFields.get(NodeRef.of(expression.getGroupingColumns().get(0))).getRelationId();

            List<Integer> columns = expression.getGroupingColumns().stream()
                    .map(NodeRef::of)
                    .peek(groupingColumn -> checkState(columnReferenceFields.containsKey(groupingColumn), "the grouping column is not in the columnReferencesField map"))
                    .map(columnReferenceFields::get)
                    .map(fieldId -> translateFieldToInteger(fieldId, relationId))
                    .collect(toImmutableList());

            List<List<Integer>> groupingSetDescriptors = groupingSets.stream()
                    .map(groupingSet -> groupingSet.stream()
                            .map(NodeRef::of)
                            .filter(columnReferenceFields::containsKey)
                            .map(columnReferenceFields::get)
                            .map(fieldId -> translateFieldToInteger(fieldId, relationId))
                            .collect(toImmutableList()))
                    .collect(toImmutableList());

            List<Expression> groupingResults = groupingSetDescriptors.stream()
                    .map(groupingSetDescriptors::indexOf)
                    .map(groupId -> String.valueOf(calculateGrouping(groupId, columns, groupingSetDescriptors)))
                    .map(LongLiteral::new)
                    .collect(toImmutableList());

            // It is necessary to add a 1 to the groupId because the underlying array is indexed starting at 1
            return new SubscriptExpression(
                    new ArrayConstructor(groupingResults),
                    new ArithmeticBinaryExpression(ADD, groupIdSymbol.get().toSymbolReference(), new GenericLiteral("BIGINT", "1")));
        }
    }

    private static int translateFieldToInteger(FieldId fieldId, RelationId requiredOriginRelationId)
    {
        // TODO: this section should be rewritten when support is added for GROUP BY columns to reference an outer scope
        checkState(fieldId.getRelationId().equals(requiredOriginRelationId), "grouping arguments must all come from the same relation");
        return fieldId.getFieldIndex();
    }

    /**
     * The grouping function is used in conjunction with GROUPING SETS, ROLLUP and CUBE to
     * indicate which columns are present in that grouping.
     *
     * <p>The grouping function must be invoked with arguments that exactly match the columns
     * referenced in the corresponding GROUPING SET, ROLLUP or CUBE clause at the associated
     * query level. Those column arguments are not evaluated and instead the function is
     * re-written with the arguments below.
     *
     * <p>To compute the resulting bit set for a particular row, bits are assigned to the
     * argument columns with the rightmost column being the most significant bit. For a
     * given grouping, a bit is set to 0 if the corresponding column is included in the
     * grouping and 1 otherwise. For an example, see the SQL documentation for the
     * function.
     *
     * @param groupId An ordinal indicating which grouping is currently being processed.
     *        Each grouping is assigned a unique monotonically increasing integer.
     * @param columns The column arguments with which the function was
     *        invoked converted to ordinals with respect to the base table column
     *        ordering.
     * @param groupingSetDescriptors A collection of ordinal lists where the index of
     *        the list is the groupId and the list itself contains the ordinals of the
     *        columns present in the grouping. For example: [[0, 2], [2], [0, 1, 2]]
     *        means the the 0th list contains the set of columns that are present in
     *        the 0th grouping.
     * @return A bit set converted to decimal indicating which columns are present in
     *         the grouping. If a column is NOT present in the grouping its corresponding
     *         bit is set to 1 and to 0 if the column is present in the grouping.
     */
    static long calculateGrouping(long groupId, List<Integer> columns, List<List<Integer>> groupingSetDescriptors)
    {
        long grouping = (1L << columns.size()) - 1;

        List<Integer> groupingSet = groupingSetDescriptors.get(toIntExact(groupId));
        for (Integer groupingColumn : groupingSet) {
            int index = columns.indexOf(groupingColumn);
            if (index != -1) {
                // Leftmost argument to grouping() (i.e. when index = 0) corresponds to
                // the most significant bit in the result. That is why we shift 1L starting
                // from the columns.size() - 1 bit index.
                grouping = grouping & ~(1L << (columns.size() - 1 - index));
            }
        }

        return grouping;
    }
}
