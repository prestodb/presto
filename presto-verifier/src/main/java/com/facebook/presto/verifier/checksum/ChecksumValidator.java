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

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.Column.Category;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ChecksumValidator
{
    private final Map<Category, Provider<ColumnValidator>> columnValidators;

    @Inject
    public ChecksumValidator(Map<Category, Provider<ColumnValidator>> columnValidators)
    {
        this.columnValidators = columnValidators;
    }

    public Query generateChecksumQuery(QualifiedName tableName, List<Column> columns)
    {
        return generateChecksumBreakdownQuery(tableName, columns, null);
    }

    public Query generateChecksumBreakdownQuery(QualifiedName tableName, List<Column> columns, Expression where)
    {
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builder();
        selectItems.add(new SingleColumn(new FunctionCall(QualifiedName.of("count"), ImmutableList.of())));
        for (Column column : columns) {
            selectItems.addAll(columnValidators.get(column.getCategory()).get().generateChecksumColumns(column));
        }
        if (where == null) {
            return simpleQuery(new Select(false, selectItems.build()), new Table(tableName));
        }
        else {
            return simpleQuery(new Select(false, selectItems.build()), new Table(tableName), where);
        }
    }

    public Query generateSelectQuery(QualifiedName tableName, List<Column> columns)
    {
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builder();
        for (Column column : columns) {
            selectItems.add(new SingleColumn(column.getExpression()));
        }
        return simpleQuery(new Select(false, selectItems.build()), new Table(tableName));
    }

    public Query generateBucketChecksumQuery(QualifiedName tableName, List<Column> columns)
    {
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builder();
        selectItems.add(new SingleColumn(new FunctionCall(QualifiedName.of("count"), ImmutableList.of())));
        for (Column column : columns) {
            selectItems.addAll(columnValidators.get(column.getCategory()).get().generateChecksumColumns(column));
        }
        Expression where = new ComparisonExpression(
                EQUAL,
                new SymbolReference("$bucket"),
                new LongLiteral("0"));

        return simpleQuery(new Select(false, selectItems.build()), new Table(tableName), where);
    }

    public List<ColumnMatchResult<?>> getMismatchedColumns(List<Column> columns, ChecksumResult controlChecksum, ChecksumResult testChecksum)
    {
        return columns.stream()
                .flatMap(column -> columnValidators.get(column.getCategory()).get().validate(column, controlChecksum, testChecksum).stream())
                .filter(columnMatchResult -> !columnMatchResult.isMatched())
                .collect(toImmutableList());
    }
}
