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
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.verifier.framework.Column;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.functionCall;
import static com.facebook.presto.verifier.checksum.ArrayColumnValidator.generateArrayChecksum;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class MapColumnValidator
        implements ColumnValidator
{
    @Inject
    public MapColumnValidator() {}

    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        checkArgument(column.getType() instanceof MapType, "Expect MapType, found %s", column.getType().getDisplayName());
        Type keyType = ((MapType) column.getType()).getKeyType();
        Type valueType = ((MapType) column.getType()).getValueType();

        Expression checksum = functionCall("checksum", column.getExpression());
        Expression keysChecksum = generateArrayChecksum(functionCall("map_keys", column.getExpression()), new ArrayType(keyType));
        Expression valuesChecksum = generateArrayChecksum(functionCall("map_values", column.getExpression()), new ArrayType(valueType));
        Expression mapCardinalitySum = new CoalesceExpression(
                functionCall("sum", functionCall("cardinality", column.getExpression())),
                new LongLiteral("0"));

        return ImmutableList.of(
                new SingleColumn(checksum, Optional.of(delimitedIdentifier(getChecksumColumnAlias(column)))),
                new SingleColumn(keysChecksum, Optional.of(delimitedIdentifier(getKeysChecksumColumnAlias(column)))),
                new SingleColumn(valuesChecksum, Optional.of(delimitedIdentifier(getValuesChecksumColumnAlias(column)))),
                new SingleColumn(mapCardinalitySum, Optional.of(delimitedIdentifier(getCardinalitySumColumnAlias(column)))));
    }

    @Override
    public List<ColumnMatchResult> validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        String checksumColumnAlias = getChecksumColumnAlias(column);
        Object controlChecksum = controlResult.getChecksum(checksumColumnAlias);
        Object testChecksum = testResult.getChecksum(checksumColumnAlias);

        String keysChecksumColumnAlias = getKeysChecksumColumnAlias(column);
        Object controlKeysChecksum = controlResult.getChecksum(keysChecksumColumnAlias);
        Object testKeysChecksum = testResult.getChecksum(keysChecksumColumnAlias);

        String valuesChecksumColumnAlias = getValuesChecksumColumnAlias(column);
        Object controlValuesChecksum = controlResult.getChecksum(valuesChecksumColumnAlias);
        Object testValuesChecksum = testResult.getChecksum(valuesChecksumColumnAlias);

        String cardinalitySumColumnAlias = getCardinalitySumColumnAlias(column);
        Object controlCardinalitySum = controlResult.getChecksum(cardinalitySumColumnAlias);
        Object testCardinalitySum = testResult.getChecksum(cardinalitySumColumnAlias);

        return ImmutableList.of(new ColumnMatchResult(
                Objects.equals(controlChecksum, testChecksum)
                        && Objects.equals(controlKeysChecksum, testKeysChecksum)
                        && Objects.equals(controlValuesChecksum, testValuesChecksum)
                        && Objects.equals(controlCardinalitySum, testCardinalitySum),
                column,
                format(
                        "control(checksum: %s, keys_checksum: %s, values_checksum: %s, cardinality_sum: %s) " +
                                "test(checksum: %s, keys_checksum: %s, values_checksum: %s, cardinality_sum: %s)",
                        controlChecksum,
                        controlKeysChecksum,
                        controlValuesChecksum,
                        controlCardinalitySum,
                        testChecksum,
                        testKeysChecksum,
                        testValuesChecksum,
                        testCardinalitySum)));
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

    private static String getCardinalitySumColumnAlias(Column column)
    {
        return column.getName() + "$cardinality_sum";
    }
}
