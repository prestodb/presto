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
package com.facebook.presto.sql.rewrite;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Created by localadmin on 8/16/18.
 */
public class RowFilterColumnMaskerRewrite
        implements StatementRewrite.Rewrite
{
    @Override
    public Statement rewrite(Session session, Metadata metadata, SqlParser parser, Optional<QueryExplainer>
            queryExplainer, Statement node, List<Expression> parameters, AccessControl accessControl)
    {
        return (Statement) new Visitor(metadata, parser, session, parameters, accessControl,
                queryExplainer).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;
        List<Expression> parameters;
        private final AccessControl accessControl;
        private Optional<QueryExplainer> queryExplainer;

        public Visitor(Metadata metadata, SqlParser sqlParser, Session session, List<Expression> parameters,
                AccessControl accessControl, Optional<QueryExplainer> queryExplainer)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.parameters = requireNonNull(parameters, "parameters is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        }

        @Override
        protected Node visitQueryBody(QueryBody node, Void context)
        {
            return node.accept(this, context);
        }

        @Override
        protected Node visitRelation(Relation node, Void context)
        {
            return node.accept(this, context);
        }

        @Override
        protected Node visitLateral(Lateral lateral, Void context)
        {
            Query query = (Query) visitQuery(lateral.getQuery(), context);
            return lateral.getLocation().isPresent() ?
                    new Lateral(query.getLocation().get(), query) :
                    new Lateral(query);
        }

        @Override
        protected Node visitIntersect(Intersect intersect, Void context)
        {
            List<Relation> relations = new ArrayList<>();
            for (Relation relation : intersect.getRelations()) {
                relations.add((Relation) visitRelation(relation, context));
            }
            return intersect.getLocation().isPresent() ?
                    new Intersect(intersect.getLocation().get(), relations, intersect.isDistinct()) :
                    new Intersect(relations, intersect.isDistinct());
        }

        @Override
        protected Node visitAliasedRelation(AliasedRelation aliasedRelation, Void context)
        {
            Relation relation = (Relation) visitRelation(aliasedRelation.getRelation(), context);
            if (aliasedRelation.getLocation().isPresent()) {
                return new AliasedRelation(aliasedRelation.getLocation().get(), relation, aliasedRelation.getAlias(), aliasedRelation.getColumnNames());
            }
            else {
                return new AliasedRelation(relation, aliasedRelation.getAlias(), aliasedRelation.getColumnNames());
            }
        }

        @Override
        protected Node visitExcept(Except except, Void context)
        {
            Relation relationLeft = (Relation) visitRelation(except.getLeft(), context);
            Relation relationRight = (Relation) visitRelation(except.getRight(), context);

            return except.getLocation().isPresent() ?
                    new Except(except.getLocation().get(), relationLeft, relationRight, except.isDistinct()) :
                    new Except(relationLeft, relationRight, except.isDistinct());
        }

        @Override
        protected Node visitJoin(Join join, Void context)
        {
            Relation relationLeft = (Relation) visitRelation(join.getLeft(), context);
            Relation relationRight = (Relation) visitRelation(join.getRight(), context);

            return join.getLocation().isPresent() ?
                    new Join(join.getLocation().get(), join.getType(), relationLeft, relationRight, join.getCriteria()) :
                    new Join(join.getType(), relationLeft, relationRight, join.getCriteria());
        }

        @Override
        protected Node visitSampledRelation(SampledRelation sampledRelation, Void context)
        {
            Relation relation = (Relation) visitRelation(sampledRelation.getRelation(), context);
            return sampledRelation.getLocation().isPresent() ?
                    new SampledRelation(sampledRelation.getLocation().get(), relation, sampledRelation.getType(), sampledRelation.getSamplePercentage()) :
                    new SampledRelation(relation, sampledRelation.getType(), sampledRelation.getSamplePercentage());
        }

        @Override
        protected Node visitSetOperation(SetOperation setOperation, Void context)
        {
            return visitNode(setOperation, context);
        }

        @Override
        protected Node visitTable(Table table, Void context)
        {
            boolean hasColumnMasking = false;
            QualifiedObjectName qualifiedObjectName = MetadataUtil.createQualifiedObjectName(session, table, table.getName());

            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, qualifiedObjectName);

            Select select = new Select(false, new
                    ArrayList<SelectItem>()
                    {
                        {
                            add(new AllColumns());
                        }
                    });

            // Adding column based masking policies
            // TODO: Add column blacklisting in case the user does not have permission on that column
            if (tableHandle.isPresent()) {
                List<SelectItem> selectItems = new ArrayList<SelectItem>();
                Map<String, ColumnHandle> columnNames = metadata.getColumnHandles(session, tableHandle.get());
                for (String columnName : columnNames.keySet()) {
                    String expStr = accessControl.applyColumnMasking(session.getTransactionId().get(), session.getIdentity(), qualifiedObjectName, columnName);
                    if (expStr == null) {
                        selectItems.add(new SingleColumn(new Identifier(columnName)));
                    }
                    else {
                        hasColumnMasking = true;
                        selectItems.add(new SingleColumn(sqlParser.createExpression(expStr)));
                    }
                }
                System.out.println(columnNames);
                select = new Select(false, selectItems);
            }

            QuerySpecification subQuerySpecification;
            String expStr = accessControl.applyRowFilters(session.getTransactionId().get(), session.getIdentity(), qualifiedObjectName);

            // do not change anything if both row and column policies are not there
            if (expStr == null && !hasColumnMasking) {
                return table;
            }
            // only col masking
            else if (expStr == null) {
                subQuerySpecification = new QuerySpecification(select, Optional.of(new
                        Table(table.getName())), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
            }
            // both row and col policies are there
            else {
                Expression expression = sqlParser.createExpression(expStr);
                subQuerySpecification = new QuerySpecification(select, Optional.of(new
                        Table(table.getName())), Optional.of(expression), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
            }

            Query query = new Query(Optional.empty(), subQuerySpecification, Optional.empty(), Optional
                    .empty());

            return new TableSubquery(query);
        }

        @Override
        protected Node visitTableSubquery(TableSubquery subquery, Void context)
        {
            Query query = (Query) visitQuery(subquery.getQuery(), context);
            return subquery.getLocation().isPresent() ?
                    new TableSubquery(subquery.getLocation().get(), query) :
                    new TableSubquery(query);
        }

        @Override
        protected Node visitUnion(Union subquery, Void context)
        {
            List<Relation> relations = new ArrayList<>();
            for (Relation relation : subquery.getRelations()) {
                relations.add((Relation) visitRelation(relation, context));
            }
            return subquery.getLocation().isPresent() ?
                    new Union(subquery.getLocation().get(), relations, subquery.isDistinct()) :
                    new Union(relations, subquery.isDistinct());
        }

        @Override
        protected Node visitValues(Values subquery, Void context)
        {
            return visitNode(subquery, context);
        }

        @Override
        protected Node visitUnnest(Unnest subquery, Void context)
        {
            return visitNode(subquery, context);
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        @Override
        protected Node visitQuery(Query node, Void context)
        {
            if (node.getLocation().isPresent()) {
                return new Query(node.getLocation().get(), node.getWith(), (QueryBody) visitQueryBody(node.getQueryBody(),
                        context), node
                        .getOrderBy(), node.getLimit());
            }
            else {
                return new Query(node.getWith(), (QueryBody) visitQueryBody(node.getQueryBody(),
                        context), node
                        .getOrderBy(), node.getLimit());
            }
        }

        @Override
        protected Node visitQuerySpecification(QuerySpecification node, Void context)
        {
            Optional<Relation> relation = Optional.empty();
            if (node.getFrom().isPresent()) {
                relation = Optional.of((Relation) visitRelation(node.getFrom().get(), context));
            }
            return node.getLocation().isPresent() ?
                    new QuerySpecification(node.getLocation().get(), node
                            .getSelect(),
                            relation,
                            node.getWhere(),
                            node.getGroupBy(),
                            node.getHaving(),
                            node.getOrderBy(),
                            node.getLimit()) :
                    new QuerySpecification(node
                            .getSelect(),
                            relation,
                            node.getWhere(),
                            node.getGroupBy(),
                            node.getHaving(),
                            node.getOrderBy(),
                            node.getLimit());
        }
    }
}
