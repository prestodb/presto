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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RewriteVisitor
        extends AstVisitor<Node, RewriteVisitor.RewriteVisitorContext>
{
    private final Session session;

    public RewriteVisitor(Session session)
    {
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public Node process(Node node, RewriteVisitorContext context)
    {
        return super.process(node, context);
    }

    @Override
    protected Node visitQuery(Query node, RewriteVisitorContext context)
    {
        return new Query(
                node.getWith(),
                (QueryBody) process(node.getQueryBody(), context),
                node.getOrderBy(),
                node.getLimit());
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, RewriteVisitorContext context)
    {
        return new TableSubquery((Query) process(node.getQuery(), context));
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, RewriteVisitorContext context)
    {
        // Finished: visitSelect()
        if (node.getFrom().isPresent()) {
            return new QuerySpecification(
                    (Select) process(node.getSelect(), context),
                    Optional.of((Relation) process(node.getFrom().get(), context)),
                    node.getWhere(),
                    node.getGroupBy(),
                    node.getHaving(),
                    node.getOrderBy(),
                    node.getLimit());
        }
        return node;
    }

    @Override
    protected Node visitSelect(Select node, RewriteVisitorContext context)
    {
        ImmutableList.Builder<SelectItem> rewriteSelectItems = ImmutableList.builder();

        for (SelectItem selectItem : node.getSelectItems()) {
            SelectItem rewriteSelectItem = (SelectItem) process(selectItem, context);
            rewriteSelectItems.add(rewriteSelectItem);
        }

        Select viewSelect = new Select(node.isDistinct(), rewriteSelectItems.build());

        return viewSelect;
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, RewriteVisitorContext context)
    {
        Expression selectExpression = node.getExpression();
        return new SingleColumn(
                (Expression) process(selectExpression, context),
                node.getAlias());
    }

    @Override
    protected Node visitAllColumns(AllColumns node, RewriteVisitorContext context)
    {
        return node;
    }

    // TODO: Handle ArithmeticBinaryExpression
    @Override
    protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, RewriteVisitorContext context)
    {
        // TODO: complete the function
        return node;
    }

    @Override
    protected Node visitIdentifier(Identifier node, RewriteVisitorContext context)
    {
        String baseColumnName = node.getValue();
        return new Identifier(context.getViewColumnName(baseColumnName), node.isDelimited());
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, RewriteVisitorContext context)
    {
        String functionCall = node.toString();
        List<Expression> rewriteArguments = new ArrayList<>();

        if (context.containsColumnName(functionCall)) {
            Expression derivedExpression = new Identifier(context.getViewColumnName(functionCall));
            rewriteArguments.add(derivedExpression);
        }
        else {
            for (Expression argument : node.getArguments()) {
                Expression rewriteArgument = (Expression) process(argument, context);
                rewriteArguments.add(rewriteArgument);
            }
        }

        return new FunctionCall(
            node.getName(),
            node.getWindow(),
            node.getFilter(),
            node.getOrderBy(),
            node.isDistinct(),
            node.isIgnoreNulls(),
            rewriteArguments);
    }

    @Override
    protected Node visitTable(Table node, RewriteVisitorContext context)
    {
        return context.getMaterializedViewTable();
    }

    protected static final class RewriteVisitorContext
    {
        private Table materializedViewTable;
        private Query originalSqlQuery;
        private Map<String, String> baseToViewColumnMap;

        public RewriteVisitorContext(
                Table materializedViewTable,
                Query originalSqlQuery)
        {
            this.materializedViewTable = materializedViewTable;
            this.originalSqlQuery = originalSqlQuery;
            baseToViewColumnMap = new HashMap<>();
            createBaseToViewColumnMap();
        }

        public void createBaseToViewColumnMap()
        {
            QuerySpecification originalSqlQueryBody = (QuerySpecification) originalSqlQuery.getQueryBody();
            Select derivedFieldsNames = originalSqlQueryBody.getSelect();

            for (SelectItem viewColumnName : derivedFieldsNames.getSelectItems()) {
                String baseColumnName = ((SingleColumn) viewColumnName).getExpression().toString();
                Optional<Identifier> viewOptionalDerivedName = ((SingleColumn) viewColumnName).getAlias();
                String viewDerivedColumnName = baseColumnName;

                if (viewOptionalDerivedName.isPresent()) {
                    viewDerivedColumnName = viewOptionalDerivedName.get().getValue();
                }

                baseToViewColumnMap.put(baseColumnName, viewDerivedColumnName);
            }
        }

        public Table getMaterializedViewTable()
        {
            return materializedViewTable;
        }

        public Query getOriginalSqlQuery()
        {
            return originalSqlQuery;
        }

        public String getViewColumnName(String baseColumnName)
        {
            checkState(baseToViewColumnMap.containsKey(baseColumnName), "Missing column name in the conversion map: " + baseColumnName);
            return baseToViewColumnMap.get(baseColumnName);
        }

        public boolean containsColumnName(String baseColumnName)
        {
            return baseToViewColumnMap.containsKey(baseColumnName);
        }
    }
}
