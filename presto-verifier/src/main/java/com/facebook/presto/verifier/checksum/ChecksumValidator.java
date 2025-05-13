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

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.Column.Category;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.UNDEFINED;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ChecksumValidator
{
    private final Map<Category, Provider<ColumnValidator>> columnValidators;

    @Inject
    public ChecksumValidator(Map<Category, Provider<ColumnValidator>> columnValidators)
    {
        this.columnValidators = columnValidators;
    }

    public Query generateChecksumQuery(QualifiedName tableName, List<Column> columns, Optional<Expression> partitionPredicate)
    {
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builder();
        selectItems.add(new SingleColumn(new FunctionCall(QualifiedName.of("count"), ImmutableList.of())));
        for (Column column : columns) {
            selectItems.addAll(columnValidators.get(column.getCategory()).get().generateChecksumColumns(column));
        }
        return simpleQuery(new Select(false, selectItems.build()), new Table(tableName), partitionPredicate, Optional.empty());
    }

    public Query generatePartitionChecksumQuery(QualifiedName tableName, List<Column> dataColumns, List<Column> partitionColumns, Optional<Expression> partitionPredicate)
    {
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builder();
        selectItems.add(new SingleColumn(new FunctionCall(QualifiedName.of("count"), ImmutableList.of())));
        for (Column column : dataColumns) {
            selectItems.addAll(columnValidators.get(column.getCategory()).get().generateChecksumColumns(column));
        }

        ImmutableList.Builder<GroupingElement> groupByList = ImmutableList.builder();
        ImmutableList.Builder<SortItem> orderByList = ImmutableList.builder();
        for (Column partitionColumn : partitionColumns) {
            orderByList.add(new SortItem(new Identifier(partitionColumn.getName()), ASCENDING, UNDEFINED));
            groupByList.add(new SimpleGroupBy(ImmutableList.of(new Identifier(partitionColumn.getName()))));
        }
        return simpleQuery(
                new Select(false, selectItems.build()),
                new Table(tableName),
                partitionPredicate,
                Optional.of(new GroupBy(false, groupByList.build())),
                Optional.empty(),
                Optional.of(new OrderBy(orderByList.build())),
                Optional.empty(),
                Optional.empty());
    }

    public Query generateBucketChecksumQuery(QualifiedName tableName, List<Column> partitionColumns, List<Column> dataColumns, Optional<Expression> partitionPredicate)
    {
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builder();
        selectItems.add(new SingleColumn(new FunctionCall(QualifiedName.of("count"), ImmutableList.of())));
        for (Column column : dataColumns) {
            selectItems.addAll(columnValidators.get(column.getCategory()).get().generateChecksumColumns(column));
        }

        ImmutableList.Builder<GroupingElement> groupByList = ImmutableList.builder();
        ImmutableList.Builder<SortItem> orderByList = ImmutableList.builder();
        for (Column partitionColumn : partitionColumns) {
            orderByList.add(new SortItem(new Identifier(partitionColumn.getName()), ASCENDING, UNDEFINED));
            groupByList.add(new SimpleGroupBy(ImmutableList.of(new Identifier(partitionColumn.getName()))));
        }
        orderByList.add(new SortItem(new Identifier("$bucket"), ASCENDING, UNDEFINED));
        groupByList.add(new SimpleGroupBy(ImmutableList.of(new Identifier("$bucket"))));
        return simpleQuery(
                new Select(false, selectItems.build()),
                new Table(tableName),
                partitionPredicate,
                Optional.of(new GroupBy(false, groupByList.build())),
                Optional.empty(),
                Optional.of(new OrderBy(orderByList.build())),
                Optional.empty(),
                Optional.empty());
    }

    public List<ColumnMatchResult<?>> getMismatchedColumns(List<Column> columns, ChecksumResult controlChecksum, ChecksumResult testChecksum)
    {
        return columns.stream()
                .flatMap(column -> columnValidators.get(column.getCategory()).get().validate(column, controlChecksum, testChecksum).stream())
                .filter(columnMatchResult -> !columnMatchResult.isMatched())
                .collect(toImmutableList());
    }
}
