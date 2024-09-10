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

import com.facebook.presto.common.type.AbstractVarcharType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static java.util.Objects.requireNonNull;

public class SimpleColumnValidator
        implements ColumnValidator
{
    private final FloatingPointColumnValidator floatingPointValidator;
    private final boolean validateStringAsDouble;

    @Inject
    public SimpleColumnValidator(VerifierConfig config, FloatingPointColumnValidator floatingPointValidator)
    {
        this.floatingPointValidator = requireNonNull(floatingPointValidator, "floatingPointValidator is null");
        this.validateStringAsDouble = config.isValidateStringAsDouble();
    }

    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        ImmutableList.Builder<SingleColumn> columnsBuilder = ImmutableList.builder();
        // checksum(column)
        Expression checksum = new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(column.getExpression()));
        columnsBuilder.add(new SingleColumn(checksum, delimitedIdentifier(getChecksumColumnAlias(column))));

        if (shouldValidateStringAsDouble(column)) {
            Column asDoubleColumn = getAsDoubleColumn(column);
            columnsBuilder.addAll(floatingPointValidator.generateChecksumColumns(asDoubleColumn));
            columnsBuilder.addAll(ColumnValidatorUtil.generateNullCountColumns(column, asDoubleColumn));
        }
        return columnsBuilder.build();
    }

    @Override
    public List<ColumnMatchResult<SimpleColumnChecksum>> validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        boolean isStringAsDouble = shouldValidateStringAsDouble(column) && ColumnValidatorUtil.isStringAsDoubleColumn(column, controlResult, testResult);

        SimpleColumnChecksum controlChecksum = toColumnChecksum(column, controlResult, isStringAsDouble);
        SimpleColumnChecksum testChecksum = toColumnChecksum(column, testResult, isStringAsDouble);

        if (isStringAsDouble) {
            Column asDoubleColumn = getAsDoubleColumn(column);
            ColumnMatchResult<FloatingPointColumnChecksum> asDoubleMatchResult =
                    floatingPointValidator.validate(asDoubleColumn, controlChecksum.getAsDoubleChecksum(), testChecksum.getAsDoubleChecksum());
            return ImmutableList.of(new ColumnMatchResult<>(asDoubleMatchResult.isMatched(), column, asDoubleMatchResult.getMessage(), controlChecksum, testChecksum));
        }

        return ImmutableList.of(new ColumnMatchResult<>(Objects.equals(controlChecksum, testChecksum), column, controlChecksum, testChecksum));
    }

    private static SimpleColumnChecksum toColumnChecksum(Column column, ChecksumResult checksumResult, boolean isStringAsDouble)
    {
        Object checksum = checksumResult.getChecksum(getChecksumColumnAlias(column));

        if (isStringAsDouble) {
            Column asDoubleColumn = getAsDoubleColumn(column);
            return new SimpleColumnChecksum(
                    checksum,
                    Optional.of(FloatingPointColumnValidator.toColumnChecksum(asDoubleColumn, checksumResult, checksumResult.getRowCount())));
        }
        return new SimpleColumnChecksum(checksum, Optional.empty());
    }

    private boolean shouldValidateStringAsDouble(Column column)
    {
        Type columnType = column.getType();
        return (validateStringAsDouble && columnType instanceof AbstractVarcharType);
    }

    private static Column getAsDoubleColumn(Column column)
    {
        return Column.create(column.getName() + "_as_double", getAsDoubleExpression(column), DOUBLE);
    }

    private static Expression getAsDoubleExpression(Column column)
    {
        return new Cast(column.getExpression(), DOUBLE.getDisplayName(), true, false);
    }

    private static String getChecksumColumnAlias(Column column)
    {
        return column.getName() + "$checksum";
    }
}
