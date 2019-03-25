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

import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.Column.Category;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.verifier.framework.Column.Category.FLOATING_POINT;
import static com.facebook.presto.verifier.framework.Column.Category.ORDERABLE_ARRAY;
import static com.facebook.presto.verifier.framework.Column.Category.SIMPLE;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;

public class ChecksumValidator
{
    private final Map<Category, ColumnValidator> columnValidators;

    @Inject
    public ChecksumValidator(
            SimpleColumnValidator simpleColumnValidator,
            FloatingPointColumnValidator floatingPointColumnValidator,
            OrderableArrayColumnValidator orderableArrayColumnValidator)
    {
        this.columnValidators = ImmutableMap.of(
                SIMPLE, simpleColumnValidator,
                FLOATING_POINT, floatingPointColumnValidator,
                ORDERABLE_ARRAY, orderableArrayColumnValidator);
    }

    public Query generateChecksumQuery(QualifiedName tableName, List<Column> columns)
    {
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builder();
        selectItems.add(new SingleColumn(new FunctionCall(QualifiedName.of("count"), ImmutableList.of())));
        for (Column column : columns) {
            selectItems.addAll(columnValidators.get(column.getCategory()).generateChecksumColumns(column));
        }
        return simpleQuery(new Select(false, selectItems.build()), new Table(tableName));
    }

    public Map<Column, ColumnMatchResult> getMismatchedColumns(List<Column> columns, ChecksumResult controlChecksum, ChecksumResult testChecksum)
    {
        return columns.stream()
                .collect(toImmutableMap(identity(), column -> columnValidators.get(column.getCategory()).validate(column, controlChecksum, testChecksum)))
                .entrySet()
                .stream()
                .filter(entry -> !entry.getValue().isMatched())
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }
}
