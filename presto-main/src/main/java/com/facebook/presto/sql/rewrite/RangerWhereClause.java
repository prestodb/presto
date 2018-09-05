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
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Created by localadmin on 8/16/18.
 */
public class RangerWhereClause
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
            if (node instanceof QuerySpecification) {
                if (((QuerySpecification) node).getFrom().get() instanceof Table) {
                    QuerySpecification nodeQuerySpecification = (QuerySpecification) node;

//                    if (((Table) ((QuerySpecification) node).getFrom().get()).getName().toString().equals("jmx" +
//                            ".current.com" +
//                            ".facebook.presto" +
//                            ".memory:*type=memorypool*"))
                    {
                        Table t = (Table) nodeQuerySpecification.getFrom().get();

                        String fullyQualifiedTableName = t.getName().toString();
                        // TODO: should be a better way to generate QualifiedObjectName. Hack as of now
                        String[] parts = fullyQualifiedTableName.split("\\.", 3);
                        QualifiedObjectName qualifiedObjectName = new QualifiedObjectName(parts[0], parts[1], parts[2]);


                        String expStr = accessControl.applyRowFilters(session.getTransactionId().get(), session.getIdentity(), qualifiedObjectName);
                        if (expStr == null) {
                            return node;
                        }
                        Expression expression=sqlParser.createExpression(expStr);


                        Select select = new Select(false, new
                                ArrayList<SelectItem>()
                                {
                                    {
                                        add(new AllColumns());
                                    }
                                });
                      //  final Optional<Expression> filter = Optional.of(new LikePredicate(new Identifier("object_name"), new StringLiteral("%reserved"), Optional.empty()));

                        QuerySpecification subQuerySpecification = new QuerySpecification(select, Optional.of(new
                                Table(t.getName())), Optional.of(expression), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

                        Query query = new Query(Optional.empty(), subQuerySpecification, Optional.empty(), Optional
                                .empty());

                        TableSubquery tableSubquery = new TableSubquery(query);

                        return new QuerySpecification(nodeQuerySpecification.getLocation().get(), nodeQuerySpecification
                                .getSelect(),
                                Optional.of(tableSubquery),
                                nodeQuerySpecification.getWhere(),
                                nodeQuerySpecification.getGroupBy(),
                                nodeQuerySpecification.getHaving(),
                                nodeQuerySpecification.getOrderBy(),
                                nodeQuerySpecification.getLimit());
                    }
                }
            }

            return node;
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
    }
}
