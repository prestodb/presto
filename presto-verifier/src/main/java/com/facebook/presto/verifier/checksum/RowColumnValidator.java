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

import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.RowType.Field;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.verifier.framework.Column;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class RowColumnValidator
        implements ColumnValidator
{
    @Inject
    public RowColumnValidator()
    {
    }

    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        checkColumnType(column);

        Expression checksum = new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(column.getIdentifier()));

        ImmutableList.Builder<SingleColumn> columnsBuilder = ImmutableList.builder();
        columnsBuilder.add(new SingleColumn(checksum, Optional.of(delimitedIdentifier(getChecksumAlias(column)))));

        List<Field> fields = getFields(column);

        for (int i = 0; i < getFields(column).size(); i++) {
            Field field = fields.get(i);

            Expression fieldExpression;

            if (field.getName().isPresent()) {
                fieldExpression = new DereferenceExpression(column.getIdentifier(), new Identifier(field.getName().get()));
            }
            else {
                fieldExpression = new SubscriptExpression(column.getIdentifier(), new LongLiteral(String.valueOf(i + 1)));
            }

            Expression fieldChecksum = new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(fieldExpression));
            columnsBuilder.add(new SingleColumn(fieldChecksum, Optional.of(delimitedIdentifier(getChecksumAlias(column, field, i)))));
        }

        return columnsBuilder.build();
    }

    @Override
    public ColumnMatchResult validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        checkColumnType(column);

        List<Field> fields = getFields(column);
        List<String> aliases = ImmutableList.<String>builder()
                .add(getChecksumAlias(column))
                .addAll(IntStream.range(0, fields.size())
                        .mapToObj(index -> getChecksumAlias(column, fields.get(index), index))
                        .collect(Collectors.toList()))
                .build();

        return new ColumnMatchResult(
                aliases.stream().allMatch(alias -> Objects.equals(controlResult.getChecksum(alias), testResult.getChecksum(alias))),
                prepareMatchMessage(aliases, controlResult, testResult));
    }

    private static void checkColumnType(Column column)
    {
        checkArgument(column.getType() instanceof RowType, "Expect RowType, found %s", column.getType().getDisplayName());
    }

    private static List<Field> getFields(Column column)
    {
        return ((RowType) column.getType()).getFields();
    }

    private static String getChecksumAlias(Column column)
    {
        return column.getName() + "_checksum";
    }

    private static String getChecksumAlias(Column column, Field field, int fieldIndex)
    {
        return column.getName() + "$" + field.getName().orElse("$col" + (fieldIndex + 1)) + "_checksum";
    }

    private static String prepareMatchMessage(List<String> aliases, ChecksumResult controlResult, ChecksumResult testResult)
    {
        aliases = aliases.stream()
                .filter(alias -> !Objects.equals(controlResult.getChecksum(alias), testResult.getChecksum(alias)))
                .collect(Collectors.toList());

        String control = aliases.stream()
                .map(alias -> alias + ": " + controlResult.getChecksum(alias))
                .collect(joining(", "));

        String test = aliases.stream()
                .map(alias -> alias + ": " + testResult.getChecksum(alias))
                .collect(joining(", "));

        return format("control(%s) test(%s)", control, test);
    }
}
