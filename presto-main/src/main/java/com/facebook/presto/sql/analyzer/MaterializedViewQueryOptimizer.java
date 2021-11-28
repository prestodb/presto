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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.relation.DomainTranslator.BASIC_COLUMN_EXTRACTOR;
import static com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import static com.facebook.presto.sql.ExpressionUtils.removeExpressionPrefix;
import static com.facebook.presto.sql.ExpressionUtils.removeGroupingElementPrefix;
import static com.facebook.presto.sql.ExpressionUtils.removeSingleColumnPrefix;
import static com.facebook.presto.sql.ExpressionUtils.removeSortItemPrefix;
import static com.facebook.presto.sql.analyzer.MaterializedViewInformationExtractor.MaterializedViewInfo;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.relational.Expressions.call;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MaterializedViewQueryOptimizer
        extends AstVisitor<Node, Void>
{
    private static final Logger logger = Logger.get(MaterializedViewQueryOptimizer.class);

    private final Metadata metadata;
    private final Session session;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final RowExpressionDomainTranslator domainTranslator;
    private final Table materializedView;
    private final Query materializedViewQuery;
    private final LogicalRowExpressions logicalRowExpressions;
    private final Set<QualifiedName> supportedFunctionCalls = ImmutableSet.of(
            QualifiedName.of("MIN"),
            QualifiedName.of("MAX"),
            QualifiedName.of("SUM"));

    private MaterializedViewInfo materializedViewInfo;
    private Optional<Identifier> removablePrefix = Optional.empty();
    private Optional<Set<Expression>> expressionsInGroupBy = Optional.empty();

    public MaterializedViewQueryOptimizer(
            Metadata metadata,
            Session session,
            SqlParser sqlParser,
            AccessControl accessControl,
            RowExpressionDomainTranslator domainTranslator,
            Table materializedView,
            Query materializedViewQuery)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.sqlParser = requireNonNull(sqlParser, "sql parser is null");
        this.accessControl = requireNonNull(accessControl, "access control is null");
        this.domainTranslator = requireNonNull(domainTranslator, "row expression domain translator is null");
        this.materializedView = requireNonNull(materializedView, "materialized view is null");
        this.materializedViewQuery = requireNonNull(materializedViewQuery, "materialized view query is null");
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                new FunctionResolution(functionAndTypeManager),
                functionAndTypeManager);
    }

    public Node rewrite(Node node)
    {
        // TODO: Implement ways to handle non-optimizable query without throw/catch. https://github.com/prestodb/presto/issues/16541
        try {
            MaterializedViewInformationExtractor materializedViewInformationExtractor = new MaterializedViewInformationExtractor();
            materializedViewInformationExtractor.process(materializedViewQuery);
            materializedViewInfo = materializedViewInformationExtractor.getMaterializedViewInfo();
            return process(node);
        }
        catch (Exception ex) {
            logger.warn(ex.getMessage());
            return node;
        }
    }

    @Override
    protected Node visitNode(Node node, Void context)
    {
        for (Node child : node.getChildren()) {
            process(child, context);
        }
        return node;
    }

    @Override
    protected Node visitQuery(Query node, Void context)
    {
        return new Query(
                node.getWith(),
                (QueryBody) process(node.getQueryBody(), context),
                node.getOrderBy(),
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, Void context)
    {
        if (!node.getFrom().isPresent()) {
            throw new IllegalStateException("Query with no From clause is not rewritable by materialized view");
        }
        Relation relation = node.getFrom().get();
        if (relation instanceof AliasedRelation) {
            removablePrefix = Optional.of(((AliasedRelation) relation).getAlias());
            relation = ((AliasedRelation) relation).getRelation();
        }
        if (!(relation instanceof Table)) {
            throw new SemanticException(NOT_SUPPORTED, node, "Relation other than Table is not supported in query optimizer");
        }
        Table baseTable = (Table) relation;
        if (!removablePrefix.isPresent()) {
            removablePrefix = Optional.of(new Identifier(baseTable.getName().toString()));
        }
        if (node.getGroupBy().isPresent()) {
            ImmutableSet.Builder<Expression> expressionsInGroupByBuilder = ImmutableSet.builder();
            for (GroupingElement element : node.getGroupBy().get().getGroupingElements()) {
                element = removeGroupingElementPrefix(element, removablePrefix);
                Optional<Set<Expression>> groupByOfMaterializedView = materializedViewInfo.getGroupBy();
                if (groupByOfMaterializedView.isPresent()) {
                    for (Expression expression : element.getExpressions()) {
                        if (!groupByOfMaterializedView.get().contains(expression) || !materializedViewInfo.getBaseToViewColumnMap().containsKey(expression)) {
                            throw new IllegalStateException(format("Grouping element %s is not present in materialized view groupBy field", element));
                        }
                    }
                }
                expressionsInGroupByBuilder.addAll(element.getExpressions());
            }
            expressionsInGroupBy = Optional.of(expressionsInGroupByBuilder.build());
        }
        // TODO: Add HAVING validation to the validator https://github.com/prestodb/presto/issues/16406
        if (node.getHaving().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Having clause is not supported in query optimizer");
        }
        if (materializedViewInfo.getWhereClause().isPresent()) {
            if (!node.getWhere().isPresent()) {
                throw new IllegalStateException("Query with no where clause is not rewritable by materialized view with where clause");
            }
            QualifiedObjectName baseTableName = createQualifiedObjectName(session, baseTable, baseTable.getName());

            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, baseTableName);
            if (!tableHandle.isPresent()) {
                throw new SemanticException(MISSING_TABLE, node, "Table does not exist");
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();

            for (ColumnHandle columnHandle : metadata.getColumnHandles(session, tableHandle.get()).values()) {
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle.get(), columnHandle);
                fields.add(Field.newUnqualified(columnMetadata.getName(), columnMetadata.getType()));
            }

            Scope scope = Scope.builder()
                    .withRelationType(RelationId.anonymous(), new RelationType(fields.build()))
                    .build();

            // Given base query's filter condition and materialized view's filter condition, the goal is to check if MV's filters contain Base's filters (Base implies MV).
            // Let base query's filter condition be A, and MV's filter condition be B.
            // One way to evaluate A implies B is to evaluate logical expression A^~B and check if the output domain is none.
            // If the resulting domain is none, then A^~B is false. Thus A implies B.
            // For more information and examples: https://fb.quip.com/WwmxA40jLMxR
            // TODO: Implement method that utilizes external SAT solver libraries. https://github.com/prestodb/presto/issues/16536
            RowExpression materializedViewWhereCondition = convertToRowExpression(materializedViewInfo.getWhereClause().get(), scope);
            RowExpression baseQueryWhereCondition = convertToRowExpression(node.getWhere().get(), scope);
            RowExpression rewriteLogicExpression = and(baseQueryWhereCondition,
                    call("not",
                            new FunctionResolution(metadata.getFunctionAndTypeManager()).notFunction(),
                            materializedViewWhereCondition.getType(),
                            materializedViewWhereCondition));
            RowExpression disjunctiveNormalForm = logicalRowExpressions.convertToDisjunctiveNormalForm(rewriteLogicExpression);
            ExtractionResult<VariableReferenceExpression> result = domainTranslator.fromPredicate(session.toConnectorSession(), disjunctiveNormalForm, BASIC_COLUMN_EXTRACTOR);

            if (!result.getTupleDomain().equals(TupleDomain.none())) {
                throw new IllegalStateException("View filter condition does not contain base query's filter condition");
            }
        }

        return new QuerySpecification(
                (Select) process(node.getSelect(), context),
                node.getFrom().map(from -> (Relation) process(from, context)),
                node.getWhere().map(where -> (Expression) process(where, context)),
                node.getGroupBy().map(groupBy -> (GroupBy) process(groupBy, context)),
                node.getHaving().map(having -> (Expression) process(having, context)),
                node.getOrderBy().map(orderBy -> (OrderBy) process(orderBy, context)),
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitSelect(Select node, Void context)
    {
        if (materializedViewInfo.isDistinct() && !node.isDistinct()) {
            throw new IllegalStateException("Materialized view has distinct and base query does not");
        }
        ImmutableList.Builder<SelectItem> rewrittenSelectItems = ImmutableList.builder();

        for (SelectItem selectItem : node.getSelectItems()) {
            rewrittenSelectItems.add((SelectItem) process(selectItem, context));
        }

        return new Select(node.isDistinct(), rewrittenSelectItems.build());
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, Void context)
    {
        // For a single table, without sub-queries, the column prefix is unnecessary. Here It is removed so that it can be mapped to the view column properly.
        // For relations other than single table, it needs to be reserved to differentiate columns from different tables.
        // One way to do so is to process the prefix within `visitDereferenceExpression()` since the prefix information is saved as `base` in `DereferenceExpression` node.
        node = removeSingleColumnPrefix(node, removablePrefix);
        Expression expression = node.getExpression();
        Optional<Set<Expression>> groupByOfMaterializedView = materializedViewInfo.getGroupBy();
        // TODO: Replace this logic with rule-based validation framework.
        if (groupByOfMaterializedView.isPresent() &&
                validateExpressionForGroupBy(groupByOfMaterializedView.get(), expression) &&
                (!expressionsInGroupBy.isPresent() || !expressionsInGroupBy.get().contains(expression))) {
            throw new IllegalStateException("Query a column presents in materialized view group by: " + expression.toString());
        }

        Expression processedColumn = (Expression) process(expression, context);
        Optional<Identifier> alias = node.getAlias();

        // If a column name was rewritten, make sure we re-alias to same name as base query
        if (!alias.isPresent() && processedColumn instanceof Identifier && !processedColumn.equals(node.getExpression())) {
            alias = Optional.of((Identifier) node.getExpression());
        }
        return new SingleColumn(processedColumn, alias);
    }

    @Override
    protected Node visitAllColumns(AllColumns node, Void context)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "All columns rewrite is not supported in query optimizer");
    }

    @Override
    protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
    {
        return new ArithmeticBinaryExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitIdentifier(Identifier node, Void context)
    {
        if (!materializedViewInfo.getBaseToViewColumnMap().containsKey(node)) {
            throw new IllegalStateException("Materialized view definition does not contain mapping for the column: " + node.getValue());
        }
        return new Identifier((materializedViewInfo.getBaseToViewColumnMap().get(node)).getValue(), node.isDelimited());
    }

    @Override
    protected Node visitExpression(Expression node, Void context)
    {
        return super.visitExpression(removeExpressionPrefix(node, removablePrefix), context);
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, Void context)
    {
        if (!supportedFunctionCalls.contains(node.getName())) {
            throw new SemanticException(NOT_SUPPORTED, node, node.getName() + " function is not supported in query optimizer");
        }
        ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();

        if (materializedViewInfo.getBaseToViewColumnMap().containsKey(node)) {
            rewrittenArguments.add(materializedViewInfo.getBaseToViewColumnMap().get(node));
        }
        else {
            for (Expression argument : node.getArguments()) {
                rewrittenArguments.add((Expression) process(argument, context));
            }
        }

        return new FunctionCall(
                node.getName(),
                node.getWindow(),
                node.getFilter(),
                node.getOrderBy(),
                node.isDistinct(),
                node.isIgnoreNulls(),
                rewrittenArguments.build());
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, Void context)
    {
        return visitRelation(node.getRelation(), context);
    }

    @Override
    protected Node visitRelation(Relation node, Void context)
    {
        if (materializedViewInfo.getBaseTable().isPresent() && node.equals(materializedViewInfo.getBaseTable().get())) {
            return materializedView;
        }
        throw new IllegalStateException("Mismatching table or non-supporting relation format in base query");
    }

    @Override
    protected Node visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
    {
        return new LogicalBinaryExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitComparisonExpression(ComparisonExpression node, Void context)
    {
        return new ComparisonExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitGroupBy(GroupBy node, Void context)
    {
        ImmutableList.Builder<GroupingElement> rewrittenGroupBy = ImmutableList.builder();
        for (GroupingElement element : node.getGroupingElements()) {
            rewrittenGroupBy.add((GroupingElement) process(removeGroupingElementPrefix(element, removablePrefix), context));
        }
        return new GroupBy(node.isDistinct(), rewrittenGroupBy.build());
    }

    @Override
    protected Node visitOrderBy(OrderBy node, Void context)
    {
        ImmutableList.Builder<SortItem> rewrittenOrderBy = ImmutableList.builder();
        for (SortItem sortItem : node.getSortItems()) {
            sortItem = removeSortItemPrefix(sortItem, removablePrefix);
            if (!materializedViewInfo.getBaseToViewColumnMap().containsKey(sortItem.getSortKey())) {
                throw new IllegalStateException(format("Sort key %s is not present in materialized view select fields", sortItem.getSortKey()));
            }
            rewrittenOrderBy.add((SortItem) process(sortItem, context));
        }
        return new OrderBy(rewrittenOrderBy.build());
    }

    @Override
    protected Node visitSortItem(SortItem node, Void context)
    {
        return new SortItem((Expression) process(node.getSortKey(), context), node.getOrdering(), node.getNullOrdering());
    }

    @Override
    protected Node visitSimpleGroupBy(SimpleGroupBy node, Void context)
    {
        ImmutableList.Builder<Expression> rewrittenSimpleGroupBy = ImmutableList.builder();
        for (Expression column : node.getExpressions()) {
            rewrittenSimpleGroupBy.add((Expression) process(removeExpressionPrefix(column, removablePrefix), context));
        }
        return new SimpleGroupBy(rewrittenSimpleGroupBy.build());
    }

    private boolean validateExpressionForGroupBy(Set<Expression> groupByOfMaterializedView, Expression expression)
    {
        // If a selected column is not present in GROUP BY node of the query.
        // It must be 1) be selected in the materialized view and 2) not present in GROUP BY node of the materialized view
        return groupByOfMaterializedView.contains(expression) || !materializedViewInfo.getBaseToViewColumnMap().containsKey(expression);
    }

    private RowExpression convertToRowExpression(Expression expression, Scope scope)
    {
        ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(
                session,
                metadata,
                accessControl,
                sqlParser,
                scope,
                new Analysis(null, ImmutableList.of(), false),
                expression,
                WarningCollector.NOOP);
        return SqlToRowExpressionTranslator.translate(
                expression,
                expressionAnalysis.getExpressionTypes(),
                ImmutableMap.of(),
                metadata.getFunctionAndTypeManager(),
                session);
    }
}
