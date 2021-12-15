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
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.relation.DomainTranslator.BASIC_COLUMN_EXTRACTOR;
import static com.facebook.presto.sql.ExpressionUtils.removeGroupingElementPrefix;
import static com.facebook.presto.sql.analyzer.MaterializedViewInformationExtractor.MaterializedViewInfo;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.relational.Expressions.call;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MaterializedViewInfoValidation extends AstVisitor<Node, Void> {

    private static final Logger logger = Logger.get(MaterializedViewInfoValidation.class);
    private static Metadata metadata;
    //private static QualifiedObjectName[] tableNames;
    private static Session session;
    private static QualifiedObjectName[] tableNames;

    // private final Metadata metadata;
    //private final Session session;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final RowExpressionDomainTranslator domainTranslator;
    private final Table materializedView;
    private final Query materializedViewQuery;
    //private final Set<QualifiedObjectName> tableNames = new HashSet<>();
    private final LogicalRowExpressions logicalRowExpressions;
    private final Set<QualifiedName> supportedFunctionCalls = ImmutableSet.of(
            QualifiedName.of("MIN"),
            QualifiedName.of("MAX"),
            QualifiedName.of("SUM"));

    private MaterializedViewInfo materializedViewInfo;
    private Optional<Identifier> removablePrefix = Optional.empty();
    private Optional<Set<Expression>> expressionsInGroupBy = Optional.empty();

    public MaterializedViewInfoValidation(
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

    public void getgroupByQuery(QuerySpecification node, Optional<Identifier> removablePrefix, MaterializedViewInfo materializedViewInfo, Optional<Set<Expression>> expressionsInGroupBy) {
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
    }

    public void getHavingQuery(QuerySpecification node, Table baseTable) {
        if (materializedViewInfo.getHavingClause().isPresent()) {
            //throw new SemanticException(NOT_SUPPORTED, node, "Having clause is not supported in query optimizer");

            if (!node.getHaving().isPresent()) {
                throw new IllegalStateException("Query with no having clause is not rewritable by materialized view with having clause");
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
            RowExpression materializedViewHavingCondition = convertToRowExpression(materializedViewInfo.getHavingClause().get(), scope);
            RowExpression baseQueryHavingCondition = convertToRowExpression(node.getHaving().get(), scope);
            RowExpression rewriteLogicExpression = and(baseQueryHavingCondition,
                    call("not",
                            new FunctionResolution(metadata.getFunctionAndTypeManager()).notFunction(),
                            materializedViewHavingCondition.getType(),
                            materializedViewHavingCondition));
            RowExpression disjunctiveNormalForm = logicalRowExpressions.convertToDisjunctiveNormalForm(rewriteLogicExpression);
            ExtractionResult<VariableReferenceExpression> result = domainTranslator.fromPredicate(session.toConnectorSession(), disjunctiveNormalForm, BASIC_COLUMN_EXTRACTOR);

            if (!result.getTupleDomain().equals(TupleDomain.none())) {
                throw new IllegalStateException("View filter condition does not contain base query's filter condition");
            }
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
            // RowExpression materializedViewWhereCondition = convertToRowExpression(materializedViewInfo.getHavingClause().get(), scope);
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
    }

    public void getWhereQuery(QuerySpecification node, Table baseTable)
    {
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
            // RowExpression materializedViewWhereCondition = convertToRowExpression(materializedViewInfo.getHavingClause().get(), scope);
            RowExpression baseQueryWhereCondition = convertToRowExpression(node.getWhere().get(), scope);
            RowExpression rewriteLogicExpression = and(baseQueryWhereCondition,
                    call("not",
                            new FunctionResolution(metadata.getFunctionAndTypeManager()).notFunction(),
                            materializedViewWhereCondition.getType(),
                            materializedViewWhereCondition));
            RowExpression disjunctiveNormalForm = logicalRowExpressions.convertToDisjunctiveNormalForm(rewriteLogicExpression);
            DomainTranslator.ExtractionResult<VariableReferenceExpression> result = domainTranslator.fromPredicate(session.toConnectorSession(), disjunctiveNormalForm, BASIC_COLUMN_EXTRACTOR);

            if (!result.getTupleDomain().equals(TupleDomain.none())) {
                throw new IllegalStateException("View filter condition does not contain base query's filter condition");
            }
        }
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

    public static Set<QualifiedObjectName> getMaterializedViewCandidates()
    {
        Set<QualifiedObjectName> materializedViewCandidates = new HashSet<>();

        for (QualifiedObjectName baseTable : tableNames) {
            List<QualifiedObjectName> materializedViews = metadata.getReferencedMaterializedViews(session, baseTable);

            if (materializedViewCandidates.isEmpty()) {
                materializedViewCandidates.addAll(materializedViews);
            }
            else {
                materializedViewCandidates.retainAll(materializedViews);
            }

            if (materializedViewCandidates.isEmpty()) {
                return materializedViewCandidates;
            }
        }

        return materializedViewCandidates;
    }
}
