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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.subquery;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Rewrite queries using materialized views and replacing them with given `Expression` in `predicates` map.
 * This is used to query non-refreshed partitions of view by directly querying the base tables. Not all query
 * shapes are supported.
 * Supported: Basic Selects and Group By, Non-lateral joins, WITH, UNNEST, VALUES, Set operations, Sampled/aliased relations
 * Not supported: Lateral joins, Subqueries in WHERE
 */
public class PredicateStitcher
        extends AstVisitor<Node, PredicateStitcher.PredicateStitcherContext>
{
    private final Map<SchemaTableName, Expression> predicates;
    private final Session session;

    public PredicateStitcher(Session session, Map<SchemaTableName, Expression> predicates)
    {
        this.session = requireNonNull(session, "session is null");
        this.predicates = requireNonNull(predicates, "predicates is null");
    }

    @Override
    protected Node visitQuery(Query node, PredicateStitcherContext context)
    {
        Optional<With> rewrittenWith = Optional.empty();
        if (node.getWith().isPresent()) {
            rewrittenWith = Optional.of((With) process(node.getWith().get(), context));
        }

        return new Query(
                rewrittenWith,
                (QueryBody) process(node.getQueryBody(), context),
                node.getOrderBy(),
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, PredicateStitcherContext context)
    {
        return new TableSubquery((Query) process(node.getQuery(), context));
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, PredicateStitcherContext context)
    {
        if (node.getFrom().isPresent()) {
            return new QuerySpecification(
                    node.getSelect(),
                    Optional.of((Relation) process(node.getFrom().get(), context)),
                    node.getWhere(),
                    node.getGroupBy(),
                    node.getHaving(),
                    node.getOrderBy(),
                    node.getOffset(),
                    node.getLimit());
        }
        return node;
    }

    @Override
    protected Node visitUnnest(Unnest node, PredicateStitcherContext context)
    {
        return node;
    }

    @Override
    protected Node visitValues(Values node, PredicateStitcherContext context)
    {
        return node;
    }

    @Override
    protected Node visitWith(With node, PredicateStitcherContext context)
    {
        List<WithQuery> rewrittenWithQueries = node.getQueries().stream()
                .map(withQuery -> (WithQuery) process(withQuery, context))
                .collect(toImmutableList());
        return new With(node.isRecursive(), rewrittenWithQueries);
    }

    @Override
    protected Node visitWithQuery(WithQuery node, PredicateStitcherContext context)
    {
        return new WithQuery(node.getName(), (Query) process(node.getQuery(), context), node.getColumnNames());
    }

    @Override
    protected Node visitJoin(Join node, PredicateStitcherContext context)
    {
        Relation rewrittenLeft = (Relation) process(node.getLeft(), context);
        Relation rewrittenRight = (Relation) process(node.getRight(), context);
        return new Join(node.getType(), rewrittenLeft, rewrittenRight, node.getCriteria());
    }

    @Override
    protected Node visitUnion(Union node, PredicateStitcherContext context)
    {
        List<Relation> rewrittenRelations = node.getRelations().stream()
                .map(relation -> (Relation) process(relation, context))
                .collect(toImmutableList());
        return new Union(rewrittenRelations, node.isDistinct());
    }

    @Override
    protected Node visitExcept(Except node, PredicateStitcherContext context)
    {
        Relation rewrittenLeft = (Relation) process(node.getLeft(), context);
        Relation rewrittenRight = (Relation) process(node.getRight(), context);
        return new Except(rewrittenLeft, rewrittenRight, node.isDistinct());
    }

    @Override
    protected Node visitIntersect(Intersect node, PredicateStitcherContext context)
    {
        List<Relation> rewrittenRelations = node.getRelations().stream()
                .map(relation -> (Relation) process(relation, context))
                .collect(toImmutableList());
        return new Intersect(rewrittenRelations, node.isDistinct());
    }

    @Override
    protected Node visitSampledRelation(SampledRelation node, PredicateStitcherContext context)
    {
        return new SampledRelation((Relation) process(node.getRelation(), context), node.getType(), node.getSamplePercentage());
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, PredicateStitcherContext context)
    {
        context.setCreateAlias(false);
        AliasedRelation aliasedRelation = new AliasedRelation((Relation) process(node.getRelation(), context), node.getAlias(), node.getColumnNames());
        context.setCreateAlias(true);
        return aliasedRelation;
    }

    @Override
    protected Node visitTable(Table table, PredicateStitcherContext context)
    {
        SchemaTableName schemaTableName = toSchemaTableName(createQualifiedObjectName(session, table, table.getName()));
        if (!predicates.containsKey(schemaTableName)) {
            return table;
        }

        QuerySpecification queryWithPredicateStitching = new QuerySpecification(
                selectList(new AllColumns()),
                Optional.of(table),
                Optional.of(predicates.get(schemaTableName)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Relation subquery = subquery(new Query(Optional.empty(), queryWithPredicateStitching, Optional.empty(), Optional.empty(), Optional.empty()));
        if (context.isCreateAlias()) {
            return new AliasedRelation(subquery, identifier(schemaTableName.getTableName()), null);
        }
        return subquery;
    }

    protected static final class PredicateStitcherContext
    {
        private boolean createAlias = true;

        public boolean isCreateAlias()
        {
            return createAlias;
        }

        public void setCreateAlias(boolean createAlias)
        {
            this.createAlias = createAlias;
        }
    }
}
