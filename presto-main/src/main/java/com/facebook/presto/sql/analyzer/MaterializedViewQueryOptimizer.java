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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.MaterializedViewUtils;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isMaterializedViewDataConsistencyEnabled;
import static com.facebook.presto.SystemSessionProperties.isMaterializedViewPartitionFilteringEnabled;
import static com.facebook.presto.common.RuntimeMetricName.MANY_PARTITIONS_MISSING_IN_MATERIALIZED_VIEW_COUNT;
import static com.facebook.presto.common.RuntimeMetricName.OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.relation.DomainTranslator.BASIC_COLUMN_EXTRACTOR;
import static com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import static com.facebook.presto.sql.ExpressionUtils.removeExpressionPrefix;
import static com.facebook.presto.sql.ExpressionUtils.removeGroupingElementPrefix;
import static com.facebook.presto.sql.ExpressionUtils.removeSingleColumnPrefix;
import static com.facebook.presto.sql.ExpressionUtils.removeSortItemPrefix;
import static com.facebook.presto.sql.MaterializedViewUtils.COUNT;
import static com.facebook.presto.sql.MaterializedViewUtils.SUM;
import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.sql.analyzer.MaterializedViewInformationExtractor.MaterializedViewInfo;
import static com.facebook.presto.sql.relational.Expressions.call;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites a query by optimizing any "leaf queries" (queries which are made from a table,
 * e.g. SELECT foo FROM t1 [ AS bar]) which can be optimized with an existing materialized view.
 * A leaf query can be a subquery, but does not need to be. {@link MaterializedViewQueryOptimizer
 * copies the input Query, handing off leaf queries to {@link QuerySpecificationRewriter#rewrite}
 * for potential rewriting
 */
public class MaterializedViewQueryOptimizer
        extends AstVisitor<Node, Void>
{
    private static final Logger log = Logger.get(MaterializedViewQueryOptimizer.class);

    private final Metadata metadata;
    private final Session session;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final RowExpressionDomainTranslator domainTranslator;
    private final LogicalRowExpressions logicalRowExpressions;

    public MaterializedViewQueryOptimizer(
            Metadata metadata,
            Session session,
            SqlParser sqlParser,
            AccessControl accessControl,
            RowExpressionDomainTranslator domainTranslator)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.sqlParser = requireNonNull(sqlParser, "sql parser is null");
        this.accessControl = requireNonNull(accessControl, "access control is null");
        this.domainTranslator = requireNonNull(domainTranslator, "row expression domain translator is null");
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                new FunctionResolution(functionAndTypeManager),
                functionAndTypeManager);
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, Void context)
    {
        if (!node.getFrom().isPresent()) {
            return node;
        }
        // If a query specification has a Table as a source, it can potentially be rewritten,
        // so hand control over to QuerySpecificationRewriter via rewriteQuerySpecificationIfCompatible
        Relation from = node.getFrom().get();
        if (from instanceof Table) {
            return rewriteQuerySpecificationIfCompatible(node, (Table) from);
        }

        if (from instanceof AliasedRelation
                && ((AliasedRelation) from).getRelation() instanceof Table) {
            return rewriteQuerySpecificationIfCompatible(node, (Table) ((AliasedRelation) from).getRelation());
        }

        Relation newFrom = processSameType(from);
        if (from == newFrom) {
            return node;
        }

        return new QuerySpecification(
                node.getSelect(),
                Optional.of(newFrom),
                node.getWhere(),
                node.getGroupBy(),
                node.getHaving(),
                node.getOrderBy(),
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitNode(Node node, Void context)
    {
        return node;
    }

    @Override
    protected Node visitQuery(Query node, Void context)
    {
        QueryBody newQueryBody = processSameType(node.getQueryBody());
        Optional<With> newWith = node.getWith().map(this::processSameType);
        boolean withSame = !newWith.isPresent() || node.getWith().get() == newWith.get();
        if (withSame && node.getQueryBody() == newQueryBody) {
            return node;
        }

        return new Query(newWith, newQueryBody, node.getOrderBy(), node.getOffset(), node.getLimit());
    }

    @Override
    protected Node visitUnion(Union node, Void context)
    {
        List<Relation> newRelations = processNodes(node.getRelations());
        if (node.getRelations() == newRelations) {
            return node;
        }

        return new Union(newRelations, node.isDistinct());
    }

    @Override
    protected Node visitIntersect(Intersect node, Void context)
    {
        List<Relation> newRelations = processNodes(node.getRelations());
        if (node.getRelations() == newRelations) {
            return node;
        }

        return new Intersect(newRelations, node.isDistinct());
    }

    @Override
    protected Node visitExcept(Except node, Void context)
    {
        Relation newLeft = processSameType(node.getLeft());
        Relation newRight = processSameType(node.getRight());
        if (newLeft == node.getLeft() && newRight == node.getRight()) {
            return node;
        }

        return new Except(newLeft, newRight, node.isDistinct());
    }

    @Override
    protected Node visitLateral(Lateral node, Void context)
    {
        Query newQuery = processSameType(node.getQuery());
        if (node.getQuery() == newQuery) {
            return node;
        }

        return new Lateral(newQuery);
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, Void context)
    {
        Query newQuery = processSameType(node.getQuery());
        if (node.getQuery() == newQuery) {
            return node;
        }

        return new TableSubquery(processSameType(newQuery));
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, Void context)
    {
        Relation newRelation = processSameType(node.getRelation());
        if (node.getRelation() == newRelation) {
            return node;
        }

        return new AliasedRelation(processSameType(newRelation), node.getAlias(), node.getColumnNames());
    }

    @Override
    protected Node visitSampledRelation(SampledRelation node, Void context)
    {
        Relation newRelation = processSameType(node.getRelation());
        if (node.getRelation() == newRelation) {
            return node;
        }

        return new SampledRelation(newRelation, node.getType(), node.getSamplePercentage());
    }

    @Override
    protected Node visitJoin(Join node, Void context)
    {
        Relation newLeft = processSameType(node.getLeft());
        Relation newRight = processSameType(node.getRight());
        if (node.getLeft() == newLeft && node.getRight() == newRight) {
            return node;
        }

        return new Join(node.getType(), newLeft, newRight, node.getCriteria());
    }

    @Override
    protected Node visitWith(With node, Void context)
    {
        List<WithQuery> newWithQueries = processNodes(node.getQueries());
        if (node.getQueries() == newWithQueries) {
            return node;
        }

        return new With(node.isRecursive(), newWithQueries);
    }

    @Override
    protected Node visitWithQuery(WithQuery node, Void context)
    {
        Query newQuery = processSameType(node.getQuery());
        if (node.getQuery() == newQuery) {
            return node;
        }

        return new WithQuery(node.getName(), processSameType(node.getQuery()), node.getColumnNames());
    }

    private <T extends Node> List<T> processNodes(List<T> nodes)
    {
        List<T> newNodes = new ArrayList<>();
        boolean listsIdentical = true;

        for (T node : nodes) {
            T newNode = processSameType(node);
            if (node != newNode) {
                listsIdentical = false;
            }
            newNodes.add(newNode);
        }
        return listsIdentical ? nodes : newNodes;
    }

    private <T extends Node> T processSameType(T node)
    {
        return (T) process(node);
    }

    private QuerySpecification rewriteQuerySpecificationIfCompatible(QuerySpecification querySpecification, Table baseTable)
    {
        Optional<String> errorMessage = MaterializedViewRewriteQueryShapeValidator.validate(querySpecification);

        if (errorMessage.isPresent()) {
            log.debug("Rewrite failed for base table %s with error message { %s }. ", baseTable.getName(), errorMessage.get());
            return querySpecification;
        }

        List<QualifiedObjectName> referencedMaterializedViews = metadata.getReferencedMaterializedViews(
                session,
                createQualifiedObjectName(session, baseTable, baseTable.getName()));

        // TODO: Select the most compatible and efficient materialized view for query rewrite optimization https://github.com/prestodb/presto/issues/16431
        // TODO: Refactor query optimization code https://github.com/prestodb/presto/issues/16759

        for (QualifiedObjectName materializedViewName : referencedMaterializedViews) {
            QuerySpecification rewrittenQuerySpecification = getRewrittenQuerySpecification(metadata, materializedViewName, querySpecification);
            if (rewrittenQuerySpecification != querySpecification) {
                return rewrittenQuerySpecification;
            }
        }
        return querySpecification;
    }

    private QuerySpecification getRewrittenQuerySpecification(Metadata metadata, QualifiedObjectName materializedViewName, QuerySpecification originalQuerySpecification)
    {
        ConnectorMaterializedViewDefinition materializedViewDefinition = metadata.getMaterializedView(session, materializedViewName).orElseThrow(() ->
                new IllegalStateException("Materialized view definition not present in metadata as expected."));
        Table materializedViewTable = new Table(QualifiedName.of(materializedViewDefinition.getTable()));
        Query materializedViewQuery = (Query) sqlParser.createStatement(materializedViewDefinition.getOriginalSql(), createParsingOptions(session));

        return new QuerySpecificationRewriter(materializedViewTable, materializedViewQuery, materializedViewName).rewrite(originalQuerySpecification);
    }

    private class QuerySpecificationRewriter
            extends AstVisitor<Node, Void>
    {
        private final Table materializedView;
        private final Query materializedViewQuery;
        private final QualifiedObjectName materializedViewName;

        private MaterializedViewInfo materializedViewInfo;
        private Optional<Identifier> removablePrefix = Optional.empty();
        private Optional<Set<Expression>> expressionsInGroupBy = Optional.empty();
        private Optional<String> errorMessage = Optional.empty();

        QuerySpecificationRewriter(
                Table materializedView,
                Query materializedViewQuery,
                QualifiedObjectName materializedViewName)
        {
            this.materializedView = requireNonNull(materializedView, "materialized view is null");
            this.materializedViewQuery = requireNonNull(materializedViewQuery, "materialized view query is null");
            this.materializedViewName = requireNonNull(materializedViewName, "materialized view name is null");
        }

        public QuerySpecification rewrite(QuerySpecification querySpecification)
        {
            MaterializedViewInformationExtractor materializedViewInformationExtractor = new MaterializedViewInformationExtractor();
            materializedViewInformationExtractor.process(materializedViewQuery);
            materializedViewInfo = materializedViewInformationExtractor.getMaterializedViewInfo();
            if (materializedViewInfo.getErrorMessage().isPresent()) {
                log.debug("Failed to rewrite query with materialized view with following exception: %s", materializedViewInfo.getErrorMessage().get());
                return querySpecification;
            }
            // TODO: Implement ways to handle non-optimizable query without throw/catch. https://github.com/prestodb/presto/issues/16541
            try {
                QuerySpecification rewrittenQuerySpecification = (QuerySpecification) process(querySpecification);

                if (errorMessage.isPresent()) {
                    log.debug("Rewrite optimization failed with: %s", errorMessage.get());
                    return querySpecification;
                }

                if (rewrittenQuerySpecification == querySpecification) {
                    return querySpecification;
                }

                if (!isMaterializedViewDataConsistencyEnabled(session)) {
                    session.getRuntimeStats().addMetricValue(OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT, NONE, 1);
                    return rewrittenQuerySpecification;
                }

                // TODO: We should be able to leverage this information in the StatementAnalyzer as well.
                MaterializedViewStatus materializedViewStatus = getMaterializedViewStatus(querySpecification);
                if (materializedViewStatus.isPartiallyMaterialized() || materializedViewStatus.isFullyMaterialized()) {
                    session.getRuntimeStats().addMetricValue(OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT, NONE, 1);
                    return rewrittenQuerySpecification;
                }
                session.getRuntimeStats().addMetricValue(MANY_PARTITIONS_MISSING_IN_MATERIALIZED_VIEW_COUNT, NONE, 1);
                return querySpecification;
            }
            catch (Exception e) {
                log.info("Materialized view optimization failed with exception: %s", e.getMessage());
                return querySpecification;
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
            Relation relation = node.getFrom().get();
            if (relation instanceof AliasedRelation) {
                removablePrefix = Optional.of(((AliasedRelation) relation).getAlias());
                relation = ((AliasedRelation) relation).getRelation();
            }
            if (!(relation instanceof Table)) {
                throw new IllegalArgumentException("visitQuerySpecification should not be invoked for a non-table FROM clause");
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
                                setErrorMessage(format("Grouping element %s is not present in materialized view groupBy field", element));
                                return null;
                            }
                        }
                    }
                    expressionsInGroupByBuilder.addAll(element.getExpressions());
                }
                expressionsInGroupBy = Optional.of(expressionsInGroupByBuilder.build());
            }
            if (materializedViewInfo.getWhereClause().isPresent()) {
                if (!node.getWhere().isPresent()) {
                    setErrorMessage("Query with no where clause is not rewritable by materialized view with where clause");
                    return null;
                }
                Scope scope = extractScope(baseTable, node, materializedViewInfo.getWhereClause().get());

                // Given base query's filter condition and materialized view's filter condition, the goal is to check if materialized view's
                // filters contain Base's filters (Base implies materialized view).
                // Let base query's filter condition be A, and materialized view's filter condition be B.
                // One way to evaluate A implies B is to evaluate logical expression A^~B and check if the output domain is none.
                // If the resulting domain is none, then A^~B is false. Thus A implies B.
                // For more information and examples: https://fb.quip.com/WwmxA40jLMxR
                // TODO: Implement method that utilizes external SAT solver libraries. https://github.com/prestodb/presto/issues/16536
                RowExpression materializedViewWhereCondition = convertToRowExpression(materializedViewInfo.getWhereClause().get(), scope);
                RowExpression baseQueryWhereCondition = convertToRowExpression(node.getWhere().get(), scope);
                RowExpression rewriteLogicExpression = and(baseQueryWhereCondition,
                        call(baseQueryWhereCondition.getSourceLocation(),
                                "not",
                                new FunctionResolution(metadata.getFunctionAndTypeManager()).notFunction(),
                                materializedViewWhereCondition.getType(),
                                materializedViewWhereCondition));
                RowExpression disjunctiveNormalForm = logicalRowExpressions.convertToDisjunctiveNormalForm(rewriteLogicExpression);
                ExtractionResult<VariableReferenceExpression> result = domainTranslator.fromPredicate(session.toConnectorSession(), disjunctiveNormalForm, BASIC_COLUMN_EXTRACTOR);

                if (!result.getTupleDomain().equals(TupleDomain.none())) {
                    setErrorMessage("View filter condition does not contain base query's filter condition");
                    return null;
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
                setErrorMessage("Materialized view has distinct and base query does not");
                return null;
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
                setErrorMessage("Query a column presents in materialized view group by: " + expression.toString());
                return null;
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
            ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();

            if (materializedViewInfo.getBaseToViewColumnMap().containsKey(node)) {
                Identifier derivedColumn = materializedViewInfo.getBaseToViewColumnMap().get(node);

                if (node.getName().equals(COUNT)) {
                    return rewriteCountAsSum(node, derivedColumn);
                }
                rewrittenArguments.add(derivedColumn);
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
            setErrorMessage("Mismatching table or non-supporting relation format in base query");
            return null;
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
                    setErrorMessage(format("Sort key %s is not present in materialized view select fields", sortItem.getSortKey()));
                    return null;
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

        /**
         * This is special-cased for now as COUNT is the only non-associative
         * function supported by materialized view rewrites. In the future, we will want to
         * support more non-associative functions and explore more
         * extensible options.
         * <p>
         * Functions in optimized materialized view queries are by default expanded to
         * func(column_derived_from_func_in_mv). This only works for associative
         * functions. Count is non-associative: COUNT(x \ union y) != COUNT(Count(x), COUNT(y)).
         * Rather, COUNT(x \ union y) == SUM(COUNT(x), COUNT(y). This is what we do here.
         */
        private FunctionCall rewriteCountAsSum(FunctionCall node, Expression derivedColumnName)
        {
            if (node.isDistinct()) {
                setErrorMessage("COUNT(DISTINCT) is not supported for materialized view query rewrite optimization");
                return null;
            }

            return new FunctionCall(
                    SUM,
                    node.getWindow(),
                    node.getFilter(),
                    node.getOrderBy(),
                    node.isDistinct(),
                    node.isIgnoreNulls(),
                    ImmutableList.of(derivedColumnName));
        }

        private RowExpression convertToRowExpression(Expression expression, Scope scope)
        {
            ExpressionAnalysis originalExpressionAnalysis = getExpressionAnalysis(expression, scope);

            // DomainTranslator#fromPredicate needs type information, so do any necessary coercions here
            Expression coercedMaybe = rewriteExpressionWithCoercions(expression, originalExpressionAnalysis);

            ExpressionAnalysis coercedExpressionAnalysis = getExpressionAnalysis(coercedMaybe, scope);

            return SqlToRowExpressionTranslator.translate(
                    coercedMaybe,
                    coercedExpressionAnalysis.getExpressionTypes(),
                    ImmutableMap.of(),
                    metadata.getFunctionAndTypeManager(),
                    session);
        }

        ExpressionAnalysis getExpressionAnalysis(Expression expression, Scope scope)
        {
            return ExpressionAnalyzer.analyzeExpression(
                    session,
                    metadata,
                    accessControl,
                    sqlParser,
                    scope,
                    new Analysis(null, ImmutableMap.of(), false),
                    expression,
                    WarningCollector.NOOP);
        }

        private Expression rewriteExpressionWithCoercions(Expression expression, ExpressionAnalysis analysis)
        {
            return ExpressionTreeRewriter.rewriteWith(new CoercionRewriter(analysis), expression, null);
        }

        private class CoercionRewriter
                extends ExpressionRewriter<Void>
        {
            private final ExpressionAnalysis analysis;

            CoercionRewriter(ExpressionAnalysis analysis)
            {
                this.analysis = analysis;
            }

            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> expressionRewriter)
            {
                Expression rewrittenExpression = expressionRewriter.defaultRewrite(node, context);
                return coerceIfNecessary(node, rewrittenExpression);
            }

            private Expression coerceIfNecessary(Expression original, Expression rewritten)
            {
                Type coercion = analysis.getCoercion(original);
                if (coercion != null) {
                    rewritten = new Cast(
                            original.getLocation(),
                            rewritten,
                            coercion.getTypeSignature().toString(),
                            false,
                            analysis.isTypeOnlyCoercion(original));
                }
                return rewritten;
            }
        }

        private Scope extractScope(Table table, QuerySpecification querySpecification, Expression whereClause)
        {
            QualifiedObjectName baseTableName = createQualifiedObjectName(session, table, table.getName());

            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, baseTableName);
            if (!tableHandle.isPresent()) {
                throw new IllegalStateException(format("Table does not exist for querySpecification: %s", querySpecification));
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();

            for (ColumnHandle columnHandle : metadata.getColumnHandles(session, tableHandle.get()).values()) {
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle.get(), columnHandle);
                fields.add(Field.newUnqualified(whereClause.getLocation(), columnMetadata.getName(), columnMetadata.getType()));
            }

            return Scope.builder()
                    .withRelationType(RelationId.anonymous(), new RelationType(fields.build()))
                    .build();
        }

        private MaterializedViewStatus getMaterializedViewStatus(QuerySpecification querySpecification)
        {
            TupleDomain<String> baseQueryDomain = TupleDomain.all();

            if (querySpecification.getWhere().isPresent() && isMaterializedViewPartitionFilteringEnabled(session)) {
                Expression baseQueryWhereClause = querySpecification.getWhere().get();

                Relation from = querySpecification.getFrom().orElseThrow(() -> new IllegalStateException("from should be present"));

                Table table;
                if (from instanceof Table) {
                    table = (Table) querySpecification.getFrom().get();
                }
                else if (from instanceof AliasedRelation && ((AliasedRelation) from).getRelation() instanceof Table) {
                    table = (Table) ((AliasedRelation) from).getRelation();
                }
                else {
                    throw new IllegalStateException("from should be either a Table or AliasedRelation with table source");
                }

                Scope filterScope = extractScope(table, querySpecification, baseQueryWhereClause);

                RowExpression rowExpression = convertToRowExpression(baseQueryWhereClause, filterScope);
                baseQueryDomain = MaterializedViewUtils.getDomainFromFilter(session, domainTranslator, rowExpression);
            }

            return metadata.getMaterializedViewStatus(session, materializedViewName, baseQueryDomain);
        }

        private void setErrorMessage(String errorMessage)
        {
            this.errorMessage = Optional.of(errorMessage);
        }
    }
}
