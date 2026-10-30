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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.MaterializedViewPartitionTransformRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionRange;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransform;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import io.airlift.slice.Slice;
import jakarta.annotation.Nullable;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransform.TRANSFORM_PROPERTY;
import static com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProperties.MappingCardinality.MANY_TO_ONE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticExceptions.missingAttributeException;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Map predicates on view columns in the RefreshMaterializedView where clause to predicates on base table columns,
 * which could be used for predicate push-down afterwards. Mapped predicates are connected by AND or OR.
 * For view columns that do not map to a base table column, keep the predicate with the view. A derived partition
 * column maps through its registered single-source transform.
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
        MaterializedViewDefinition viewDefinition = metadata.getMetadataResolver(session).getMaterializedView(viewName)
                .orElseThrow(() -> new MaterializedViewNotFoundException(toSchemaTableName(viewName)));

        Visitor visitor = new Visitor(viewDefinition, viewScope, metadata, session, originalPredicate);
        visitor.process(originalPredicate);

        return visitor.getTablePredicates();
    }

    @VisibleForTesting
    static Expression expandPartitionTransformComparison(
            MaterializedViewPartitionTransform transform,
            String baseColumnName,
            ComparisonExpression.Operator operator,
            ConstantExpression targetValue,
            LiteralEncoder literalEncoder)
    {
        Expression literal = Visitor.toExpression(targetValue, literalEncoder);
        return Visitor.expandOntoSourceRange("testColumn", transform, baseColumnName, operator, targetValue, literalEncoder, literal);
    }

    /**
     * Return a table to predicates map. Map key is materialized view name or base table name.
     */
    private static class Visitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        private final ImmutableMultimap.Builder<SchemaTableName, Expression> tablePredicatesBuilder = ImmutableMultimap.builder();

        private final MaterializedViewDefinition viewDefinition;
        private final Scope viewScope;
        private final Metadata metadata;
        private final Session session;
        private final LiteralEncoder literalEncoder;
        private final boolean extractPredicates;
        private final Map<String, MaterializedViewPartitionTransform> partitionTransforms;
        private final Map<String, Map<SchemaTableName, String>> transformedColumnMappings;

        private Visitor(
                MaterializedViewDefinition viewDefinition,
                Scope viewScope,
                Metadata metadata,
                Session session,
                Node errorNode)
        {
            this(viewDefinition, viewScope, metadata, session, true, errorNode);
        }

        private Visitor(
                MaterializedViewDefinition viewDefinition,
                Scope viewScope,
                Metadata metadata,
                Session session,
                boolean extractPredicates,
                Node errorNode)
        {
            this.viewDefinition = requireNonNull(viewDefinition, "viewDefinition is null");
            this.viewScope = requireNonNull(viewScope, "viewScope is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
            this.extractPredicates = extractPredicates;

            MaterializedViewPartitionTransformRegistry registry = metadata.getFunctionAndTypeManager().getMaterializedViewPartitionTransformRegistry();
            Map<String, MaterializedViewPartitionTransform> transforms = new HashMap<>();
            Map<String, Map<SchemaTableName, String>> mappings = new HashMap<>();
            for (MaterializedViewDefinition.ColumnMapping mapping : viewDefinition.getColumnMappings()) {
                if (!mapping.isDerived() || mapping.getBaseTableColumns().size() != 1) {
                    continue;
                }
                MaterializedViewDefinition.TableColumn sourceColumn = mapping.getBaseTableColumns().get(0);
                if (!sourceColumn.getColumnMappingInfo().containsKey(TRANSFORM_PROPERTY)) {
                    continue;
                }
                MaterializedViewPartitionTransform transform;
                try {
                    transform = registry.decode(sourceColumn.getColumnMappingInfo())
                            .orElseThrow(() -> new IllegalArgumentException("partition transform metadata has no transform identifier"));
                }
                catch (IllegalArgumentException | NullPointerException e) {
                    throw new SemanticException(
                            NOT_SUPPORTED,
                            errorNode,
                            "Invalid materialized view partition transform metadata for column %s: %s",
                            mapping.getViewColumn().getColumnName(),
                            e.getMessage());
                }
                transforms.put(mapping.getViewColumn().getColumnName(), transform);
                mappings.put(
                        mapping.getViewColumn().getColumnName(),
                        ImmutableMap.of(sourceColumn.getTableName(), sourceColumn.getColumnName()));
            }
            this.partitionTransforms = ImmutableMap.copyOf(transforms);
            this.transformedColumnMappings = ImmutableMap.copyOf(mappings);
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

        private Map<SchemaTableName, String> resolveBaseTableColumns(String column)
        {
            Map<SchemaTableName, String> directColumns = viewDefinition.getColumnMappingsAsMap().get(column);
            if (directColumns != null) {
                return directColumns;
            }
            return transformedColumnMappings.get(column);
        }

        @Override
        public Void process(Node node, @Nullable Void context)
        {
            if (!(node instanceof ComparisonExpression || node instanceof LogicalBinaryExpression || node instanceof InPredicate)) {
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
            if (LogicalBinaryExpression.Operator.OR.equals(node.getOperator()) && extractPredicates) {
                new Visitor(viewDefinition, viewScope, metadata, session, false, node).process(node);
                Optional<Map.Entry<SchemaTableName, Expression>> pushed = tryPushDisjunction(node);
                if (pushed.isPresent()) {
                    tablePredicatesBuilder.put(pushed.get().getKey(), pushed.get().getValue());
                }
                else {
                    SchemaTableName viewName = new SchemaTableName(viewDefinition.getSchema(), viewDefinition.getTable());
                    tablePredicatesBuilder.put(viewName, node);
                }
                return null;
            }

            if (!(node.getLeft() instanceof ComparisonExpression || node.getLeft() instanceof LogicalBinaryExpression || node.getLeft() instanceof InPredicate)) {
                throw new SemanticException(NOT_SUPPORTED, node.getLeft(), "Only column specifications connected by logical AND are supported in WHERE clause.");
            }
            if (!(node.getRight() instanceof ComparisonExpression || node.getRight() instanceof LogicalBinaryExpression || node.getRight() instanceof InPredicate)) {
                throw new SemanticException(NOT_SUPPORTED, node.getRight(), "Only column specifications connected by logical AND are supported in WHERE clause.");
            }

            return super.visitLogicalBinaryExpression(node, null);
        }

        @Override
        protected Void visitInPredicate(InPredicate node, Void context)
        {
            Expression value = node.getValue();
            Expression valueList = node.getValueList();

            if (!(value instanceof Identifier || value instanceof DereferenceExpression)) {
                throw new SemanticException(NOT_SUPPORTED, value, "Only column references are supported on the left side of IN predicates in WHERE clause.");
            }
            if (!(valueList instanceof InListExpression)) {
                throw new SemanticException(NOT_SUPPORTED, valueList, "Only IN list expressions are supported in WHERE clause's IN predicates.");
            }

            InListExpression inListExpression = (InListExpression) valueList;
            for (Expression inValue : inListExpression.getValues()) {
                if (!(inValue instanceof Literal)) {
                    throw new SemanticException(NOT_SUPPORTED, inValue, "Only literal values are supported in WHERE clause's IN lists.");
                }
            }

            QualifiedName qualifiedName = value instanceof DereferenceExpression
                    ? DereferenceExpression.getQualifiedName((DereferenceExpression) value)
                    : QualifiedName.of(((Identifier) value).getValue());

            ResolvedField resolvedField = viewScope.tryResolveField(value).orElseThrow(() -> missingAttributeException(value, qualifiedName));
            String column = resolvedField.getField().getOriginColumnName().orElseThrow(() -> missingAttributeException(value, qualifiedName));
            Type columnType = resolvedField.getField().getType();

            if (!viewDefinition.getValidRefreshColumns().orElse(emptyList()).contains(column)) {
                throw new SemanticException(NOT_SUPPORTED, value, "Refresh materialized view by column %s is not supported.", value.toString());
            }

            if (!extractPredicates) {
                return null;
            }

            Map<SchemaTableName, String> baseTableColumns = resolveBaseTableColumns(column);

            // Convert IN predicate to OR'd equality comparisons
            boolean mappedToSingleBaseTablePartition = true;
            for (Expression inValue : inListExpression.getValues()) {
                if (baseTableColumns != null && inValue instanceof NullLiteral) {
                    if (viewDefinition.getBaseTablesOnOuterJoinSide().stream().anyMatch(t -> baseTableColumns.containsKey(t))) {
                        mappedToSingleBaseTablePartition = false;
                        break;
                    }
                }
            }

            if (mappedToSingleBaseTablePartition && baseTableColumns != null) {
                for (SchemaTableName baseTable : baseTableColumns.keySet()) {
                    Expression orExpression = null;
                    for (Expression inValue : inListExpression.getValues()) {
                        Expression comparison = pushComparison(
                                column,
                                baseTableColumns.get(baseTable),
                                ComparisonExpression.Operator.EQUAL,
                                inValue,
                                columnType,
                                node);
                        if (orExpression == null) {
                            orExpression = comparison;
                        }
                        else {
                            orExpression = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR, orExpression, comparison);
                        }
                    }
                    tablePredicatesBuilder.put(baseTable, orExpression);
                }
            }
            else {
                SchemaTableName viewName = new SchemaTableName(viewDefinition.getSchema(), viewDefinition.getTable());
                Expression orExpression = null;
                for (Expression inValue : inListExpression.getValues()) {
                    ComparisonExpression comparison = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, value, inValue);
                    if (orExpression == null) {
                        orExpression = comparison;
                    }
                    else {
                        orExpression = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR, orExpression, comparison);
                    }
                }
                tablePredicatesBuilder.put(viewName, orExpression);
            }

            return null;
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, Void context)
        {
            if (!(node.getLeft() instanceof Identifier || node.getLeft() instanceof DereferenceExpression)) {
                throw new SemanticException(NOT_SUPPORTED, node.getLeft(), "Only column references are supported on the left side of comparison expressions in WHERE clause.");
            }
            if (!(node.getRight() instanceof Literal)) {
                throw new SemanticException(NOT_SUPPORTED, node.getRight(), "Only literal values are supported on the right side of comparison expressions in WHERE clause.");
            }
            QualifiedName qualifiedName = node.getLeft() instanceof DereferenceExpression
                    ? DereferenceExpression.getQualifiedName((DereferenceExpression) node.getLeft())
                    : QualifiedName.of(((Identifier) node.getLeft()).getValue());

            ResolvedField resolvedField = viewScope.tryResolveField(node.getLeft()).orElseThrow(() -> missingAttributeException(node.getLeft(), qualifiedName));
            String column = resolvedField.getField().getOriginColumnName().orElseThrow(() -> missingAttributeException(node.getLeft(), qualifiedName));
            Type columnType = resolvedField.getField().getType();

            if (!viewDefinition.getValidRefreshColumns().orElse(emptyList()).contains(column)) {
                throw new SemanticException(NOT_SUPPORTED, node.getLeft(), "Refresh materialized view by column %s is not supported.", node.getLeft().toString());
            }

            if (!extractPredicates) {
                return null;
            }

            Map<SchemaTableName, String> baseTableColumns = resolveBaseTableColumns(column);

            // Check if refresh condition can be mapped to a particular base table partition column. This can always be done unless base table
            // is part of an outer join. If that is so, we leave it to filter pushdowns to find correct base table partitions.
            boolean mappedToSingleBaseTablePartition = true;
            if (baseTableColumns != null && node.getRight() instanceof NullLiteral) {
                if (viewDefinition.getBaseTablesOnOuterJoinSide().stream().anyMatch(t -> baseTableColumns.containsKey(t))) {
                    mappedToSingleBaseTablePartition = false;
                }
            }

            if (mappedToSingleBaseTablePartition && baseTableColumns != null) {
                for (SchemaTableName baseTable : baseTableColumns.keySet()) {
                    tablePredicatesBuilder.put(
                            baseTable,
                            pushComparison(column, baseTableColumns.get(baseTable), node.getOperator(), node.getRight(), columnType, node));
                }
            }
            else {
                SchemaTableName viewName = new SchemaTableName(viewDefinition.getSchema(), viewDefinition.getTable());
                tablePredicatesBuilder.put(viewName, node);
            }

            return null;
        }

        private Expression pushComparison(
                String column,
                String baseColumnName,
                ComparisonExpression.Operator operator,
                Expression literal,
                Type columnType,
                Node errorNode)
        {
            MaterializedViewPartitionTransform transform = partitionTransforms.get(column);
            if (literal instanceof NullLiteral || transform == null || transform.getProperties().getMappingCardinality() != MANY_TO_ONE) {
                return new ComparisonExpression(operator, new Identifier(baseColumnName), literal);
            }
            ConstantExpression targetValue;
            try {
                Object value = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), (Literal) literal);
                targetValue = new ConstantExpression(Optional.empty(), value, columnType);
            }
            catch (RuntimeException e) {
                throw new SemanticException(
                        NOT_SUPPORTED,
                        errorNode,
                        "Invalid partition value %s for transformed column %s: %s",
                        literal,
                        column,
                        e.getMessage());
            }
            return expandOntoSourceRange(column, transform, baseColumnName, operator, targetValue, literalEncoder, errorNode);
        }

        private static Expression expandOntoSourceRange(
                String column,
                MaterializedViewPartitionTransform transform,
                String baseColumnName,
                ComparisonExpression.Operator operator,
                ConstantExpression targetValue,
                LiteralEncoder literalEncoder,
                Node errorNode)
        {
            Identifier baseColumn = new Identifier(baseColumnName);
            MaterializedViewPartitionRange sourceRange = getSourceRange(column, transform, targetValue, literalEncoder, errorNode);
            Expression rangeStart = toExpression(sourceRange.getLowerBound(), literalEncoder);
            Expression rangeEnd = toExpression(sourceRange.getUpperBound(), literalEncoder);

            switch (operator) {
                case EQUAL:
                    return and(atLeast(baseColumn, rangeStart), below(baseColumn, rangeEnd));
                case NOT_EQUAL:
                    return or(below(baseColumn, rangeStart), atLeast(baseColumn, rangeEnd));
                case GREATER_THAN_OR_EQUAL:
                    return atLeast(baseColumn, rangeStart);
                case LESS_THAN:
                    return below(baseColumn, rangeStart);
                case GREATER_THAN:
                    return atLeast(baseColumn, rangeEnd);
                case LESS_THAN_OR_EQUAL:
                    return below(baseColumn, rangeEnd);
                default:
                    throw new SemanticException(
                            NOT_SUPPORTED,
                            errorNode,
                            "Operator %s is not supported on transformed partition column %s. A many-to-one transform maps each target value "
                                    + "to a half-open interval of source partitions, so only =, <>, <, <=, > and >= "
                                    + "can be expanded onto that interval.",
                            operator,
                            column);
            }
        }

        private static MaterializedViewPartitionRange getSourceRange(
                String column,
                MaterializedViewPartitionTransform transform,
                ConstantExpression targetValue,
                LiteralEncoder literalEncoder,
                Node errorNode)
        {
            try {
                if (!transform.isCanonicalTargetValue(targetValue)) {
                    ConstantExpression canonicalValue = transform.apply(targetValue);
                    Expression value = toExpression(targetValue, literalEncoder);
                    Expression canonical = toExpression(canonicalValue, literalEncoder);
                    throw new SemanticException(
                            NOT_SUPPORTED,
                            errorNode,
                            "Value %s is not canonical for transformed partition column %s. Canonical value: %s",
                            value,
                            column,
                            canonical);
                }
                return transform.getSourceRange(targetValue);
            }
            catch (SemanticException e) {
                throw e;
            }
            catch (RuntimeException e) {
                throw new SemanticException(
                        NOT_SUPPORTED,
                        errorNode,
                        "Cannot map partition value for transformed column %s to an exact source range: %s",
                        column,
                        e.getMessage());
            }
        }

        private static Expression toExpression(ConstantExpression value, LiteralEncoder literalEncoder)
        {
            if (value.getType() instanceof VarcharType && value.getValue() instanceof Slice) {
                return new StringLiteral(((Slice) value.getValue()).toStringUtf8());
            }
            return literalEncoder.toExpression(value.getValue(), value.getType());
        }

        private Optional<Map.Entry<SchemaTableName, Expression>> tryPushDisjunction(Expression node)
        {
            if (node instanceof LogicalBinaryExpression) {
                LogicalBinaryExpression binary = (LogicalBinaryExpression) node;
                Optional<Map.Entry<SchemaTableName, Expression>> left = tryPushDisjunction(binary.getLeft());
                Optional<Map.Entry<SchemaTableName, Expression>> right = tryPushDisjunction(binary.getRight());
                if (!left.isPresent() || !right.isPresent() || !left.get().getKey().equals(right.get().getKey())) {
                    return Optional.empty();
                }
                return Optional.of(new AbstractMap.SimpleEntry<>(
                        left.get().getKey(),
                        new LogicalBinaryExpression(binary.getOperator(), left.get().getValue(), right.get().getValue())));
            }

            if (node instanceof ComparisonExpression) {
                ComparisonExpression comparison = (ComparisonExpression) node;
                if (!(comparison.getLeft() instanceof Identifier || comparison.getLeft() instanceof DereferenceExpression)
                        || !(comparison.getRight() instanceof Literal)) {
                    return Optional.empty();
                }
                Optional<MappedColumn> target = singleBaseTableColumn(comparison.getLeft());
                if (!target.isPresent()) {
                    return Optional.empty();
                }
                try {
                    return Optional.of(new AbstractMap.SimpleEntry<>(
                            target.get().getBaseTable(),
                            pushComparison(
                                    target.get().getViewColumn(),
                                    target.get().getBaseColumn(),
                                    comparison.getOperator(),
                                    comparison.getRight(),
                                    target.get().getType(),
                                    comparison)));
                }
                catch (SemanticException e) {
                    return Optional.empty();
                }
            }

            if (node instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) node;
                if (!(inPredicate.getValueList() instanceof InListExpression)) {
                    return Optional.empty();
                }
                List<Expression> values = ((InListExpression) inPredicate.getValueList()).getValues();
                if (values.isEmpty() || values.stream().anyMatch(value -> !(value instanceof Literal))) {
                    return Optional.empty();
                }
                Optional<MappedColumn> target = singleBaseTableColumn(inPredicate.getValue());
                if (!target.isPresent()) {
                    return Optional.empty();
                }
                try {
                    return Optional.of(new AbstractMap.SimpleEntry<>(
                            target.get().getBaseTable(),
                            values.stream()
                                    .map(value -> pushComparison(
                                            target.get().getViewColumn(),
                                            target.get().getBaseColumn(),
                                            ComparisonExpression.Operator.EQUAL,
                                            value,
                                            target.get().getType(),
                                            inPredicate))
                                    .reduce(Visitor::or)
                                    .orElseThrow(() -> new IllegalStateException("empty IN list"))));
                }
                catch (SemanticException e) {
                    return Optional.empty();
                }
            }

            return Optional.empty();
        }

        private Optional<MappedColumn> singleBaseTableColumn(Expression columnRef)
        {
            Optional<ResolvedField> resolvedField = viewScope.tryResolveField(columnRef);
            if (!resolvedField.isPresent()) {
                return Optional.empty();
            }
            Optional<String> columnName = resolvedField.get().getField().getOriginColumnName();
            if (!columnName.isPresent()
                    || !viewDefinition.getValidRefreshColumns().orElse(emptyList()).contains(columnName.get())) {
                return Optional.empty();
            }
            Map<SchemaTableName, String> baseTableColumns = resolveBaseTableColumns(columnName.get());
            if (baseTableColumns == null || baseTableColumns.size() != 1) {
                return Optional.empty();
            }
            SchemaTableName baseTable = baseTableColumns.keySet().iterator().next();
            if (viewDefinition.getBaseTablesOnOuterJoinSide().contains(baseTable)) {
                return Optional.empty();
            }
            return Optional.of(new MappedColumn(
                    baseTable,
                    columnName.get(),
                    baseTableColumns.get(baseTable),
                    resolvedField.get().getField().getType()));
        }

        private static final class MappedColumn
        {
            private final SchemaTableName baseTable;
            private final String viewColumn;
            private final String baseColumn;
            private final Type type;

            private MappedColumn(SchemaTableName baseTable, String viewColumn, String baseColumn, Type type)
            {
                this.baseTable = requireNonNull(baseTable, "baseTable is null");
                this.viewColumn = requireNonNull(viewColumn, "viewColumn is null");
                this.baseColumn = requireNonNull(baseColumn, "baseColumn is null");
                this.type = requireNonNull(type, "type is null");
            }

            private SchemaTableName getBaseTable()
            {
                return baseTable;
            }

            private String getViewColumn()
            {
                return viewColumn;
            }

            private String getBaseColumn()
            {
                return baseColumn;
            }

            private Type getType()
            {
                return type;
            }
        }

        private static Expression atLeast(Expression left, Expression right)
        {
            return new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, left, right);
        }

        private static Expression below(Expression left, Expression right)
        {
            return new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, left, right);
        }

        private static Expression and(Expression left, Expression right)
        {
            return new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, left, right);
        }

        private static Expression or(Expression left, Expression right)
        {
            return new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR, left, right);
        }
    }
}
