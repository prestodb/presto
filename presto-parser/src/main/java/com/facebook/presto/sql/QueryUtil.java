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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;

public final class QueryUtil
{
    private QueryUtil() {}

    public static Expression nameReference(String name)
    {
        return new QualifiedNameReference(QualifiedName.of(name));
    }

    public static SelectItem unaliasedName(String name)
    {
        return new SingleColumn(nameReference(name));
    }

    public static SelectItem aliasedName(String name, String alias)
    {
        return new SingleColumn(nameReference(name), alias);
    }

    public static Select selectList(Expression... expressions)
    {
        ImmutableList.Builder<SelectItem> items = ImmutableList.builder();
        for (Expression expression : expressions) {
            items.add(new SingleColumn(expression));
        }
        return new Select(false, items.build());
    }

    public static Select selectList(SelectItem... items)
    {
        return new Select(false, ImmutableList.copyOf(items));
    }

    public static Select selectAll(List<SelectItem> items)
    {
        return new Select(false, items);
    }

    public static Table table(QualifiedName name)
    {
        return new Table(name);
    }

    public static Relation subquery(Query query)
    {
        return new TableSubquery(query);
    }

    public static SortItem ascending(String name)
    {
        return new SortItem(nameReference(name), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED);
    }

    public static Expression logicalAnd(Expression left, Expression right)
    {
        return new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, left, right);
    }

    public static Expression equal(Expression left, Expression right)
    {
        return new ComparisonExpression(ComparisonExpressionType.EQUAL, left, right);
    }

    public static Expression caseWhen(Expression operand, Expression result)
    {
        return new SearchedCaseExpression(ImmutableList.of(new WhenClause(operand, result)), Optional.empty());
    }

    public static Expression functionCall(String name, Expression... arguments)
    {
        return new FunctionCall(QualifiedName.of(name), ImmutableList.copyOf(arguments));
    }

    public static Values values(Row... row)
    {
        return new Values(ImmutableList.copyOf(row));
    }

    public static Row row(Expression... values)
    {
        return new Row(ImmutableList.copyOf(values));
    }

    public static Relation aliased(Relation relation, String alias, List<String> columnAliases)
    {
        return new AliasedRelation(relation, alias, columnAliases);
    }

    public static SelectItem aliasedNullToEmpty(String column, String alias)
    {
        return new SingleColumn(new CoalesceExpression(nameReference(column), new StringLiteral("")), alias);
    }

    public static List<SortItem> ordering(SortItem... items)
    {
        return ImmutableList.copyOf(items);
    }

    public static Query simpleQuery(Select select)
    {
        return query(new QuerySpecification(
                select,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty()));
    }

    public static Query simpleQuery(Select select, Relation from)
    {
        return simpleQuery(select, from, Optional.empty(), ImmutableList.of());
    }

    public static Query simpleQuery(Select select, Relation from, List<SortItem> ordering)
    {
        return simpleQuery(select, from, Optional.empty(), ordering);
    }

    public static Query simpleQuery(Select select, Relation from, Expression where)
    {
        return simpleQuery(select, from, Optional.of(where), ImmutableList.of());
    }

    public static Query simpleQuery(Select select, Relation from, Expression where, List<SortItem> ordering)
    {
        return simpleQuery(select, from, Optional.of(where), ordering);
    }

    public static Query simpleQuery(Select select, Relation from, Optional<Expression> where, List<SortItem> ordering)
    {
        return simpleQuery(select, from, where, Optional.empty(), Optional.empty(), ordering, Optional.empty());
    }

    public static Query simpleQuery(Select select, Relation from, Optional<Expression> where, Optional<GroupBy> groupBy, Optional<Expression> having, List<SortItem> ordering, Optional<String> limit)
    {
        return query(new QuerySpecification(
                select,
                Optional.of(from),
                where,
                groupBy,
                having,
                ordering,
                limit));
    }

    public static Query singleValueQuery(String columnName, String value)
    {
        Relation values = values(row(new StringLiteral((value))));
        return simpleQuery(
                selectList(new AllColumns()),
                aliased(values, "t", ImmutableList.of(columnName)));
    }

    public static Query singleValueQuery(String columnName, boolean value)
    {
        Relation values = values(row(value ? TRUE_LITERAL : FALSE_LITERAL));
        return simpleQuery(
                selectList(new AllColumns()),
                aliased(values, "t", ImmutableList.of(columnName)));
    }

    public static Query query(QueryBody body)
    {
        return new Query(
                Optional.empty(),
                body,
                ImmutableList.of(),
                Optional.empty());
    }
}
