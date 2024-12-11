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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.verifier.framework.Column;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.sql.QueryUtil.functionCall;
import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.base.Preconditions.checkArgument;

public class ColumnValidatorUtil
{
    private static final Logger LOG = Logger.get(ColumnValidatorUtil.class);

    private ColumnValidatorUtil() {}

    public static List<SingleColumn> generateNullCountColumns(Column column, Column asDoubleColumn)
    {
        if (column.getType() instanceof ArrayType) {
            checkArgument(asDoubleColumn.getType() instanceof ArrayType, "Expect ArrayType, found %s", asDoubleColumn.getType().getDisplayName());
            // sum(cardinality(filter(column, x -> x is null)))
            return ImmutableList.of(
                    new SingleColumn(
                            functionCall("sum", functionCall("cardinality", functionCall("filter",
                                    column.getExpression(),
                                    new LambdaExpression(ImmutableList.of(new LambdaArgumentDeclaration(identifier("x"))), new IsNullPredicate(identifier("x")))))),
                            delimitedIdentifier(getNullCountColumnAlias(column))),
                    new SingleColumn(
                            functionCall("sum", functionCall("cardinality", functionCall("filter",
                                    asDoubleColumn.getExpression(),
                                    new LambdaExpression(ImmutableList.of(new LambdaArgumentDeclaration(identifier("x"))), new IsNullPredicate(identifier("x")))))),
                            delimitedIdentifier(getNullCountColumnAlias(asDoubleColumn))));
        }
        else {
            // count_if(column is null).
            return ImmutableList.of(
                    new SingleColumn(functionCall("count_if",
                            new IsNullPredicate(column.getExpression())), delimitedIdentifier(getNullCountColumnAlias(column))),
                    new SingleColumn(functionCall("count_if",
                            new IsNullPredicate(asDoubleColumn.getExpression())), delimitedIdentifier(getNullCountColumnAlias(asDoubleColumn))));
        }
    }

    // Assume both controlResult and testResult contain the results of one column count_if(column is null) and one column count_if(try_cast(column as double) is null). If the
    // results of the two columns are equal in controlResult as well as testResult, it indicates all varchar values of the column are able to be casted to double and returns
    // true. Otherwise, returns false.
    public static boolean isStringAsDoubleColumn(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        return Objects.equals(controlResult.getChecksum(getNullCountColumnAlias(column)), controlResult.getChecksum(getAsDoubleNullCountColumnAlias(column))) &&
                Objects.equals(testResult.getChecksum(getNullCountColumnAlias(column)), testResult.getChecksum(getAsDoubleNullCountColumnAlias(column)));
    }

    public static String getNullCountColumnAlias(Column column)
    {
        return column.getName() + "$null_count";
    }

    public static String getAsDoubleNullCountColumnAlias(Column column)
    {
        return column.getName() + "_as_double$null_count";
    }
}
