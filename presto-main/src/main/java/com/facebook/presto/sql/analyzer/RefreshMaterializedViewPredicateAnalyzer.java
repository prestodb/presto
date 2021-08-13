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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticExceptions.missingAttributeException;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Map predicates on view columns in the RefreshMaterializedView where clause to predicates on base table columns,
 * which could be used for predicate push-down afterwards. Mapped predicates are connected by AND.
 * For view columns that do not have a direct mapping to a base table column, keep the predicate with the view.
 */
public class RefreshMaterializedViewPredicateAnalyzer
{
    private RefreshMaterializedViewPredicateAnalyzer() {}

    public static Map<SchemaTableName, Expression> extractTablePredicates(
            QualifiedObjectName viewName,
            Expression originalPredicate,
            Scope viewScope,
            Metadata metadata,
            Session session)
    {
        ConnectorMaterializedViewDefinition viewDefinition = metadata.getMaterializedView(session, viewName)
                .orElseThrow(() -> new MaterializedViewNotFoundException(toSchemaTableName(viewName)));

        Visitor visitor = new Visitor(viewDefinition, viewScope);
        visitor.process(originalPredicate);

        return visitor.getTablePredicates();
    }

    /**
     * Return a table to predicates map. Map key is materialized view name or base table name.
     */
    private static class Visitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        private final ImmutableMultimap.Builder<SchemaTableName, Expression> tablePredicatesBuilder = ImmutableMultimap.builder();

        private final ConnectorMaterializedViewDefinition viewDefinition;
        private final Scope viewScope;

        private Visitor(
                ConnectorMaterializedViewDefinition viewDefinition,
                Scope viewScope)
        {
            this.viewDefinition = requireNonNull(viewDefinition, "viewDefinition is null");
            this.viewScope = requireNonNull(viewScope, "viewScope is null");
        }

        public Map<SchemaTableName, Expression> getTablePredicates()
        {
            ImmutableMap.Builder<SchemaTableName, Expression> tableConjuncts = ImmutableMap.builder();

            tablePredicatesBuilder.build().asMap().forEach((table, predicateCollection) -> {
                Optional<Expression> conjunctOptional = predicateCollection.stream()
                        .reduce((left, right) -> new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, left, right));

                conjunctOptional.ifPresent(conjunct -> tableConjuncts.put(table, conjunct));
            });

            return tableConjuncts.build();
        }

        @Override
        public Void process(Node node, @Nullable Void context)
        {
            if (!(node instanceof ComparisonExpression || node instanceof LogicalBinaryExpression)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Only column specifications connected by logical AND are supported in WHERE clause.");
            }

            return super.process(node, null);
        }

        @Override
        protected Void visitExpression(Expression node, Void context)
        {
            throw new SemanticException(NOT_SUPPORTED, node, "Only column specifications connected by logical AND are supported in WHERE clause.");
        }

        @Override
        protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            if (!LogicalBinaryExpression.Operator.AND.equals(node.getOperator())) {
                throw new SemanticException(NOT_SUPPORTED, node, "Only logical AND is supported in WHERE clause.");
            }
            if (!(node.getLeft() instanceof ComparisonExpression || node.getLeft() instanceof LogicalBinaryExpression)) {
                throw new SemanticException(NOT_SUPPORTED, node.getLeft(), "Only column specifications connected by logical AND are supported in WHERE clause.");
            }
            if (!(node.getRight() instanceof ComparisonExpression || node.getRight() instanceof LogicalBinaryExpression)) {
                throw new SemanticException(NOT_SUPPORTED, node.getRight(), "Only column specifications connected by logical AND are supported in WHERE clause.");
            }

            return super.visitLogicalBinaryExpression(node, null);
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, Void context)
        {
            if (!(node.getLeft() instanceof Identifier || node.getLeft() instanceof DereferenceExpression)) {
                throw new SemanticException(NOT_SUPPORTED, node.getLeft(), "Only columns specified on literals are supported in WHERE clause.");
            }
            if (!(node.getRight() instanceof Literal)) {
                throw new SemanticException(NOT_SUPPORTED, node.getRight(), "Only columns specified on literals are supported in WHERE clause.");
            }

            ResolvedField resolvedField = viewScope.tryResolveField(node.getLeft()).orElseThrow(() -> missingAttributeException(node.getLeft()));
            String column = resolvedField.getField().getOriginColumnName().orElseThrow(() -> missingAttributeException(node.getLeft()));

            if (!viewDefinition.getValidRefreshColumns().orElse(emptyList()).contains(column)) {
                throw new SemanticException(NOT_SUPPORTED, node.getLeft(), "Refresh materialized view by column %s is not supported.", node.getLeft().toString());
            }

            Map<SchemaTableName, String> baseTableColumns = viewDefinition.getColumnMappingsAsMap().get(column);
            if (baseTableColumns != null) {
                for (SchemaTableName baseTable : baseTableColumns.keySet()) {
                    tablePredicatesBuilder.put(
                            baseTable,
                            new ComparisonExpression(node.getOperator(), new Identifier(baseTableColumns.get(baseTable)), node.getRight()));
                }
            }
            else {
                SchemaTableName viewName = new SchemaTableName(viewDefinition.getSchema(), viewDefinition.getTable());
                tablePredicatesBuilder.put(viewName, node);
            }

            return null;
        }
    }
}
