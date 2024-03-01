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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
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

import static com.facebook.presto.sql.QueryUtil.functionCall;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.base.Preconditions.checkArgument;

public class ArrayColumnValidator
        implements ColumnValidator
{
    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        Expression checksum = generateArrayChecksum(column.getExpression(), column.getType());
        Expression arrayCardinalityChecksum = functionCall("checksum", functionCall("cardinality", column.getExpression()));
        Expression arrayCardinalitySum = new CoalesceExpression(
                functionCall("sum", functionCall("cardinality", column.getExpression())),
                new LongLiteral("0"));

        return ImmutableList.of(
                new SingleColumn(checksum, Optional.of(delimitedIdentifier(getChecksumColumnAlias(column)))),
                new SingleColumn(arrayCardinalityChecksum, Optional.of(delimitedIdentifier(getCardinalityChecksumColumnAlias(column)))),
                new SingleColumn(arrayCardinalitySum, Optional.of(delimitedIdentifier(getCardinalitySumColumnAlias(column)))));
    }

    @Override
    public List<ColumnMatchResult<ArrayColumnChecksum>> validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        ArrayColumnChecksum controlChecksum = toColumnChecksum(column, controlResult);
        ArrayColumnChecksum testChecksum = toColumnChecksum(column, testResult);

        return ImmutableList.of(new ColumnMatchResult<>(Objects.equals(controlChecksum, testChecksum), column, controlChecksum, testChecksum));
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

    private static ArrayColumnChecksum toColumnChecksum(Column column, ChecksumResult checksumResult)
    {
        return new ArrayColumnChecksum(
                checksumResult.getChecksum(getChecksumColumnAlias(column)),
                checksumResult.getChecksum(getCardinalityChecksumColumnAlias(column)),
                (long) checksumResult.getChecksum(getCardinalitySumColumnAlias(column)));
    }

    private static String getChecksumColumnAlias(Column column)
    {
        return column.getName() + "$checksum";
    }

    private static String getCardinalityChecksumColumnAlias(Column column)
    {
        return column.getName() + "$cardinality_checksum";
    }

    private static String getCardinalitySumColumnAlias(Column column)
    {
        return column.getName() + "$cardinality_sum";
    }
}
