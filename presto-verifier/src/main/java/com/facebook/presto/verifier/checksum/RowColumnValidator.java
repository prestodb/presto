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

import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.RowType.Field;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.verifier.framework.Column;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.base.Preconditions.checkArgument;

public class RowColumnValidator
        implements ColumnValidator
{
    private final Map<Column.Category, Provider<ColumnValidator>> columnValidators;

    @Inject
    public RowColumnValidator(Map<Column.Category, Provider<ColumnValidator>> columnValidators)
    {
        this.columnValidators = columnValidators;
    }

    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        checkColumnType(column);

        ImmutableList.Builder<SingleColumn> columnsBuilder = ImmutableList.builder();

        List<Field> fields = getFields(column);

        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            Column fieldColumn = getFieldAsColumn(column, field, i);
            List<SingleColumn> fieldColumns = columnValidators.get(fieldColumn.getCategory()).get().generateChecksumColumns(fieldColumn);
            columnsBuilder.addAll(fieldColumns);
        }

        return columnsBuilder.build();
    }

    @Override
    public List<ColumnMatchResult<?>> validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        checkColumnType(column);

        ImmutableList.Builder<ColumnMatchResult<?>> resultsBuilder = ImmutableList.builder();
        List<Field> fields = getFields(column);
        for (int i = 0; i < fields.size(); i++) {
            Column fieldColumn = getFieldAsColumn(column, fields.get(i), i);
            resultsBuilder.addAll(columnValidators.get(fieldColumn.getCategory()).get().validate(fieldColumn, controlResult, testResult));
        }

        return resultsBuilder.build();
    }

    private static void checkColumnType(Column column)
    {
        checkArgument(column.getType() instanceof RowType, "Expect RowType, found %s", column.getType().getDisplayName());
    }

    private static List<Field> getFields(Column column)
    {
        return ((RowType) column.getType()).getFields();
    }

    private static Column getFieldAsColumn(Column column, Field field, int fieldIndex)
    {
        Expression fieldExpression = field.getName()
                .<Expression>map(name -> new DereferenceExpression(column.getExpression(), delimitedIdentifier(field.getName().get())))
                .orElseGet(() -> new SubscriptExpression(column.getExpression(), new LongLiteral(String.valueOf(fieldIndex + 1))));

        return Column.create(column.getName() + "." + field.getName().orElse("_col" + (fieldIndex + 1)), fieldExpression, field.getType());
    }
}
