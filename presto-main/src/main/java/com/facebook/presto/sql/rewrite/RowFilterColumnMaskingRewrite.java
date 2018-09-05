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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Insert;
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
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Created by localadmin on 8/16/18.
 */
public class RowFilterColumnMaskingRewrite
        implements StatementRewrite.Rewrite
{
    private static final Logger log = Logger.get(RowFilterColumnMaskingRewrite.class);

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

        protected Node visitWith(With node, Void context)
        {
            List<WithQuery> withQueries = new ArrayList<>();
            for (WithQuery withQuery : node.getQueries()) {
                withQueries.add((WithQuery) visitWithQuery(withQuery, context));
            }
            return node.getLocation().isPresent() ?
                    new With(node.getLocation().get(), node.isRecursive(), withQueries) :
                    new With(node.isRecursive(), withQueries);
        }

        protected Node visitWithQuery(WithQuery node, Void context)
        {
            Query query = (Query) visitQuery(node.getQuery(), context);

            return node.getLocation().isPresent() ?
                    new WithQuery(node.getLocation().get(), node.getName(), query, node.getColumnNames()) :
                    new WithQuery(node.getName(), query, node.getColumnNames());
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
            // TODO: Handle views
            boolean hasColumnMasking = false;

            if (!MetadataUtil.tableExists(metadata, session, table.getName().getSuffix())) {
                return table;
            }

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
            if (tableHandle.isPresent()) {
                List<SelectItem> selectItems = new ArrayList<SelectItem>();
                Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

                for (Map.Entry<String, ColumnHandle> columnHandleEntry : columnHandles.entrySet()) {
                    ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle.get(), columnHandleEntry.getValue());
                    if (!columnMetadata.isHidden()) {
                        String columnName = columnMetadata.getName();
                        // TODO: Remove hack of exception check
                        try {
                            accessControl.checkCanSelectFromColumns(session.getTransactionId().get(), session.getIdentity(), qualifiedObjectName, new HashSet<String>()
                            {{ add(columnName); }});
                        }
                        catch (AccessDeniedException e) {
                            log.debug("ignoring column " + columnName + " for column masking as user " + session.getUser() + " does not have access ", e);
                            continue;
                        }
                        String expStr = accessControl.applyColumnMasking(session.getTransactionId().get(), session.getIdentity(), qualifiedObjectName, columnName);
                        if (expStr == null) {
                            selectItems.add(new SingleColumn(new Identifier(columnName)));
                        }
                        else {
                            hasColumnMasking = true;
                            selectItems.add(new SingleColumn(sqlParser.createExpression(expStr), Optional.of(new Identifier(columnName))));
                        }
                    }
                }
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
            return new AliasedRelation(new TableSubquery(query), new Identifier(table.getName().toString()), null);
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
            Optional<With> with = node.getWith();
            if (with.isPresent()) {
                with = Optional.of((With) visitWith(with.get(), context));
            }

            if (node.getLocation().isPresent()) {
                return new Query(node.getLocation().get(), with, (QueryBody) visitQueryBody(node.getQueryBody(),
                        context), node
                        .getOrderBy(), node.getLimit());
            }
            else {
                return new Query(with, (QueryBody) visitQueryBody(node.getQueryBody(),
                        context), node
                        .getOrderBy(), node.getLimit());
            }
        }

        @Override
        protected Node visitCreateTableAsSelect(CreateTableAsSelect createTableAsSelect, Void context)
        {
            Query query = (Query) visitQuery(createTableAsSelect.getQuery(), context);
            return createTableAsSelect.getLocation().isPresent() ?
                    new CreateTableAsSelect(createTableAsSelect.getLocation().get(), createTableAsSelect.getName(), query,
                            createTableAsSelect.isNotExists(), createTableAsSelect.getProperties(), createTableAsSelect.isWithData(),
                            createTableAsSelect.getColumnAliases(), createTableAsSelect.getComment()) :
                    new CreateTableAsSelect(createTableAsSelect.getName(), query, createTableAsSelect.isNotExists(),
                            createTableAsSelect.getProperties(), createTableAsSelect.isWithData(), createTableAsSelect.getColumnAliases(),
                            createTableAsSelect.getComment());
        }

        @Override
        protected Node visitInsert(Insert insert, Void context)
        {
            return new Insert(insert.getTarget(), insert.getColumns(), (Query) visitQuery(insert.getQuery(), context));
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
