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
package com.facebook.presto.verifier.checksum;

import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.verifier.framework.Column;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class ArrayColumnValidator
        implements ColumnValidator
{
    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        Expression checksum = generateArrayChecksum(column.getExpression(), column.getType());
        Expression arrayCardinalitySum = new CoalesceExpression(
                new FunctionCall(
                        QualifiedName.of("sum"),
                        ImmutableList.of(new FunctionCall(QualifiedName.of("cardinality"), ImmutableList.of(column.getExpression())))),
                new LongLiteral("0"));

        return ImmutableList.of(
                new SingleColumn(checksum, Optional.of(delimitedIdentifier(getChecksumColumnAlias(column)))),
                new SingleColumn(arrayCardinalitySum, Optional.of(delimitedIdentifier(getCardinalitySumColumnAlias(column)))));
    }

    @Override
    public List<ColumnMatchResult> validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        String checksumColumnAlias = getChecksumColumnAlias(column);
        Object controlChecksum = controlResult.getChecksum(checksumColumnAlias);
        Object testChecksum = testResult.getChecksum(checksumColumnAlias);

        String cardinalitySumColumnAlias = getCardinalitySumColumnAlias(column);
        Object controlCardinalitySum = controlResult.getChecksum(cardinalitySumColumnAlias);
        Object testCardinalitySum = testResult.getChecksum(cardinalitySumColumnAlias);

        return ImmutableList.of(new ColumnMatchResult(
                Objects.equals(controlChecksum, testChecksum) && Objects.equals(controlCardinalitySum, testCardinalitySum),
                column,
                format(
                        "control(checksum: %s, cardinality_sum: %s) test(checksum: %s, cardinality_sum: %s)",
                        controlChecksum,
                        controlCardinalitySum,
                        testChecksum,
                        testCardinalitySum)));
    }

    public static Expression generateArrayChecksum(Expression column, Type type)
    {
        checkArgument(type instanceof ArrayType, "Expect ArrayType, found %s", type.getDisplayName());
        Type elementType = ((ArrayType) type).getElementType();

        if (elementType.isOrderable()) {
            FunctionCall arraySort = new FunctionCall(QualifiedName.of("array_sort"), ImmutableList.of(column));

            if (elementType instanceof ArrayType || elementType instanceof RowType) {
                return new CoalesceExpression(
                        new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(new TryExpression(arraySort))),
                        new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(column)));
            }
            return new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(arraySort));
        }
        return new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(column));
    }

    private static String getChecksumColumnAlias(Column column)
    {
        return column.getName() + "$checksum";
    }

    private static String getCardinalitySumColumnAlias(Column column)
    {
        return column.getName() + "$cardinality_sum";
    }
}
