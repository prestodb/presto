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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.functionCall;
import static com.facebook.presto.verifier.checksum.ArrayColumnValidator.generateArrayChecksum;
import static com.facebook.presto.verifier.checksum.ArrayColumnValidator.getAsDoubleArrayColumn;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MapColumnValidator
        implements ColumnValidator
{
    private final FloatingPointColumnValidator floatingPointValidator;
    private final boolean validateStringAsDouble;

    @Inject
    public MapColumnValidator(VerifierConfig config, FloatingPointColumnValidator floatingPointValidator)
    {
        this.floatingPointValidator = requireNonNull(floatingPointValidator, "floatingPointValidator is null");
        this.validateStringAsDouble = config.isValidateStringAsDouble();
    }

    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        checkArgument(column.getType() instanceof MapType, "Expect MapType, found %s", column.getType().getDisplayName());
        Type keyType = ((MapType) column.getType()).getKeyType();
        Type valueType = ((MapType) column.getType()).getValueType();
        ImmutableList.Builder<SingleColumn> builder = ImmutableList.builder();

        Expression checksum = functionCall("checksum", column.getExpression());
        Expression keysChecksum = generateArrayChecksum(functionCall("map_keys", column.getExpression()), new ArrayType(keyType));
        // checksum(cardinality(map_column))
        Expression mapCardinalityChecksum = functionCall("checksum", functionCall("cardinality", column.getExpression()));
        // coalesce(sum(cardinality(map_column)), 0)
        Expression mapCardinalitySum = new CoalesceExpression(
                functionCall("sum", functionCall("cardinality", column.getExpression())),
                new LongLiteral("0"));

        builder.add(new SingleColumn(checksum, Optional.of(delimitedIdentifier(getChecksumColumnAlias(column)))));
        builder.add(new SingleColumn(keysChecksum, Optional.of(delimitedIdentifier(getKeysChecksumColumnAlias(column)))));

        // We need values checksum in one case only: when key is a floating point type and value is not.
        // In such case, when both column checksum and the key checksum do not match, we cannot tell if the values match or not.
        // Meaning we cannot resolve column mismatch, because value type is not a floating type and a mismatch in the values could indicate a real correctness issue.
        // In order to resolve column mismatch in such a situation, generate an extra checksum for the values.
        if ((isFloatingPointType(keyType) || shouldValidateStringAsDouble(keyType)) && !isFloatingPointType(valueType)) {
            Expression valuesChecksum = generateArrayChecksum(functionCall("map_values", column.getExpression()), new ArrayType(valueType));
            builder.add(new SingleColumn(valuesChecksum, Optional.of(delimitedIdentifier(getValuesChecksumColumnAlias(column)))));
        }

        if (shouldValidateStringAsDouble(keyType)) {
            Column keysColumn = getKeysColumn(column);
            builder.addAll(ArrayColumnValidator.generateStringArrayChecksumColumns(keysColumn));
        }
        if (shouldValidateStringAsDouble(valueType)) {
            Column valuesColumn = getValuesColumn(column);
            builder.addAll(ArrayColumnValidator.generateStringArrayChecksumColumns(valuesColumn));
        }

        builder.add(new SingleColumn(mapCardinalityChecksum, Optional.of(delimitedIdentifier(getCardinalityChecksumColumnAlias(column)))));
        builder.add(new SingleColumn(mapCardinalitySum, Optional.of(delimitedIdentifier(getCardinalitySumColumnAlias(column)))));

        return builder.build();
    }

    @Override
    public List<ColumnMatchResult<MapColumnChecksum>> validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        checkArgument(
                controlResult.getRowCount() == testResult.getRowCount(),
                "Test row count (%s) does not match control row count (%s)",
                testResult.getRowCount(),
                controlResult.getRowCount());

        Type keyType = ((MapType) column.getType()).getKeyType();
        Type valueType = ((MapType) column.getType()).getValueType();
        Column keysColumn = getKeysColumn(column);
        Column valuesColumn = getValuesColumn(column);

        boolean isDoubleKeyAsString = shouldValidateStringAsDouble(keyType) && ColumnValidatorUtil.isStringAsDoubleColumn(keysColumn, controlResult, testResult);
        boolean isDoubleValueAsString = shouldValidateStringAsDouble(valueType) && ColumnValidatorUtil.isStringAsDoubleColumn(valuesColumn, controlResult, testResult);
        MapColumnChecksum controlChecksum = toColumnChecksum(column, controlResult, isDoubleKeyAsString, isDoubleValueAsString);
        MapColumnChecksum testChecksum = toColumnChecksum(column, testResult, isDoubleKeyAsString, isDoubleValueAsString);

        if (!isDoubleKeyAsString && !isDoubleValueAsString) {
            return ImmutableList.of(new ColumnMatchResult<>(Objects.equals(controlChecksum, testChecksum), column, controlChecksum, testChecksum));
        }

        if (!Objects.equals(controlChecksum.getCardinalityChecksum(), testChecksum.getCardinalityChecksum()) ||
                !Objects.equals(controlChecksum.getCardinalitySum(), testChecksum.getCardinalitySum())) {
            return ImmutableList.of(new ColumnMatchResult<>(false, column, Optional.of("cardinality mismatch"), controlChecksum, testChecksum));
        }

        Optional<String> errorMessage = Optional.empty();
        boolean isKeyMatched = Objects.equals(controlChecksum.getKeysChecksum(), testChecksum.getKeysChecksum());
        if (isDoubleKeyAsString) {
            ColumnMatchResult<FloatingPointColumnChecksum> result =
                    floatingPointValidator.validate(getAsDoubleArrayColumn(keysColumn), controlChecksum.getKeysFloatingPointChecksum(), testChecksum.getKeysFloatingPointChecksum());
            isKeyMatched = isKeyMatched || result.isMatched();
            if (result.getMessage().isPresent()) {
                errorMessage = Optional.of("Map key " + result.getMessage().get());
            }
        }
        boolean isValueMatched = Objects.equals(controlChecksum.getValuesChecksum(), testChecksum.getValuesChecksum());
        if (isDoubleValueAsString) {
            ColumnMatchResult<FloatingPointColumnChecksum> result =
                    floatingPointValidator.validate(getAsDoubleArrayColumn(valuesColumn), controlChecksum.getValuesFloatingPointChecksum(), testChecksum.getValuesFloatingPointChecksum());
            isValueMatched = isValueMatched || result.isMatched();
            if (result.getMessage().isPresent()) {
                errorMessage = errorMessage.isPresent() ?
                        Optional.of(errorMessage.get() + ", map value " + result.getMessage().get()) :
                        Optional.of("Map value " + result.getMessage().get());
            }
        }

        return ImmutableList.of(new ColumnMatchResult<>(isKeyMatched && isValueMatched, column, errorMessage, controlChecksum, testChecksum));
    }

    private MapColumnChecksum toColumnChecksum(Column column, ChecksumResult checksumResult, boolean isDoubleKeyAsString, boolean isDoubleValueAsString)
    {
        Type keyType = ((MapType) column.getType()).getKeyType();
        Type valueType = ((MapType) column.getType()).getValueType();

        Optional<FloatingPointColumnChecksum> keysFloatingPointChecksum = Optional.empty();
        if (isDoubleKeyAsString) {
            Column keysColumn = getAsDoubleArrayColumn(getKeysColumn(column));
            keysFloatingPointChecksum = Optional.of(FloatingPointColumnValidator.toColumnChecksum(keysColumn, checksumResult, checksumResult.getRowCount()));
        }
        Optional<FloatingPointColumnChecksum> valuesFloatingPointChecksum = Optional.empty();
        if (isDoubleValueAsString) {
            Column valuesColumn = getAsDoubleArrayColumn(getValuesColumn(column));
            valuesFloatingPointChecksum = Optional.of(FloatingPointColumnValidator.toColumnChecksum(valuesColumn, checksumResult, checksumResult.getRowCount()));
        }
        Object valuesChecksum = null;
        if ((isFloatingPointType(keyType) || isDoubleKeyAsString) && !isFloatingPointType(valueType)) {
            valuesChecksum = checksumResult.getChecksum(getValuesChecksumColumnAlias(column));
        }

        if (checksumResult.getRowCount() == 0) {
            return new MapColumnChecksum(null, null, null, keysFloatingPointChecksum, valuesFloatingPointChecksum, null, 0);
        }

        return new MapColumnChecksum(
                checksumResult.getChecksum(getChecksumColumnAlias(column)),
                checksumResult.getChecksum(getKeysChecksumColumnAlias(column)),
                valuesChecksum,
                keysFloatingPointChecksum,
                valuesFloatingPointChecksum,
                checksumResult.getChecksum(getCardinalityChecksumColumnAlias(column)),
                (long) checksumResult.getChecksum(getCardinalitySumColumnAlias(column)));
    }

    private boolean shouldValidateStringAsDouble(Type columnType)
    {
        return validateStringAsDouble && columnType instanceof AbstractVarcharType;
    }

    private static boolean isFloatingPointType(Type type)
    {
        return type instanceof DoubleType || type instanceof RealType;
    }

    private static Column getKeysColumn(Column column)
    {
        checkArgument(column.getType() instanceof MapType, "Expect MapType, found %s", column.getType().getDisplayName());
        Type keyType = ((MapType) column.getType()).getKeyType();
        return Column.create(column.getName() + "_key_array", functionCall("map_keys", column.getExpression()), new ArrayType(keyType));
    }

    private static Column getValuesColumn(Column column)
    {
        checkArgument(column.getType() instanceof MapType, "Expect MapType, found %s", column.getType().getDisplayName());
        Type valueType = ((MapType) column.getType()).getValueType();
        return Column.create(column.getName() + "_value_array", functionCall("map_values", column.getExpression()), new ArrayType(valueType));
    }

    private static String getChecksumColumnAlias(Column column)
    {
        return column.getName() + "$checksum";
    }

    private static String getKeysChecksumColumnAlias(Column column)
    {
        return column.getName() + "$keys_checksum";
    }

    private static String getValuesChecksumColumnAlias(Column column)
    {
        return column.getName() + "$values_checksum";
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
