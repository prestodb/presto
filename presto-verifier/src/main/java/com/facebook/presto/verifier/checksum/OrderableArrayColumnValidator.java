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
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.verifier.framework.Column;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class OrderableArrayColumnValidator
        implements ColumnValidator
{
    @Inject
    public OrderableArrayColumnValidator()
    {
    }

    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        checkArgument(column.getType() instanceof ArrayType, "Expect ArrayType, found %s", column.getType().getDisplayName());
        Type elementType = ((ArrayType) column.getType()).getElementType();

        FunctionCall arraySort = new FunctionCall(QualifiedName.of("array_sort"), ImmutableList.of(column.getIdentifier()));
        Expression checksum;
        if (elementType instanceof ArrayType || elementType instanceof RowType) {
            checksum = new CoalesceExpression(
                    new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(new TryExpression(arraySort))),
                    new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(column.getIdentifier())));
        }
        else {
            checksum = new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(arraySort));
        }

        return ImmutableList.of(new SingleColumn(checksum, Optional.of(delimitedIdentifier(getChecksumColumnAlias(column)))));
    }

    @Override
    public ColumnMatchResult validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        String sortedChecksumColumnAlias = getChecksumColumnAlias(column);
        Object controlSortedChecksum = controlResult.getChecksum(sortedChecksumColumnAlias);
        Object testSortedChecksum = testResult.getChecksum(sortedChecksumColumnAlias);
        return new ColumnMatchResult(
                Objects.equals(controlSortedChecksum, testSortedChecksum),
                format("control(checksum: %s) test(checksum: %s)", controlSortedChecksum, testSortedChecksum));
    }

    private static String getChecksumColumnAlias(Column column)
    {
        return column.getName() + "_checksum";
    }
}
