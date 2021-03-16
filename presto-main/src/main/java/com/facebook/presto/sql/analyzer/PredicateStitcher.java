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
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Table;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.subquery;
import static java.util.Objects.requireNonNull;

public class PredicateStitcher
        extends AstVisitor<Node, Void>
{
    private final Map<SchemaTableName, Expression> predicates;
    private final Session session;

    public PredicateStitcher(Session session, Map<SchemaTableName, Expression> predicates)
    {
        this.session = requireNonNull(session, "session is null");
        this.predicates = requireNonNull(predicates, "predicates is null");
    }

    @Override
    public Node process(Node node, Void context)
    {
        return super.process(node, context);
    }

    @Override
    protected Node visitQuery(Query node, Void context)
    {
        return new Query(
                node.getWith(),
                (QueryBody) process(node.getQueryBody(), context),
                node.getOrderBy(),
                node.getLimit());
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, Void context)
    {
        if (node.getFrom().isPresent()) {
            return new QuerySpecification(
                    node.getSelect(),
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
    protected Node visitJoin(Join node, Void context)
    {
        Relation rewrittenLeft = (Relation) process(node.getLeft(), context);
        Relation rewrittenRight = (Relation) process(node.getRight(), context);
        return new Join(node.getType(), rewrittenLeft, rewrittenRight, node.getCriteria());
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, Void context)
    {
        return new AliasedRelation((Relation) process(node.getRelation(), context), node.getAlias(), node.getColumnNames());
    }

    @Override
    protected Node visitTable(Table table, Void context)
    {
        SchemaTableName schemaTableName = toSchemaTableName(createQualifiedObjectName(session, table, table.getName()));
        if (!predicates.containsKey(schemaTableName)) {
            return table;
        }

        QuerySpecification queryWithPredicateStitching = new QuerySpecification(
                selectList(new AllColumns()),
                Optional.of(table),
                Optional.of(predicates.remove(schemaTableName)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        return subquery(new Query(Optional.empty(), queryWithPredicateStitching, Optional.empty(), Optional.empty()));
    }
}
