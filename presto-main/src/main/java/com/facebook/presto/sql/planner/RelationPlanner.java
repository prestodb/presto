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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.RelationId;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.optimizations.JoinNodeUtils;
import com.facebook.presto.sql.planner.optimizations.SampleNodeUtil;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.EnumLiteral;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.UnmodifiableIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.TypeUtils.isEnumType;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.createSymbolReference;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getSourceLocation;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.isEqualComparisonExpression;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.resolveEnumLiteral;
import static com.facebook.presto.sql.analyzer.SemanticExceptions.notSupportedException;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.tree.Join.Type.INNER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

class RelationPlanner
        extends DefaultTraversalVisitor<RelationPlan, Void>
{
    private final Analysis analysis;
    private final PlanVariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> lambdaDeclarationToVariableMap;
    private final Metadata metadata;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;

    RelationPlanner(
            Analysis analysis,
            PlanVariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> lambdaDeclarationToVariableMap,
            Metadata metadata,
            Session session)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToVariableMap, "lambdaDeclarationToVariableMap is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToVariableMap = lambdaDeclarationToVariableMap;
        this.metadata = metadata;
        this.session = session;
        this.subqueryPlanner = new SubqueryPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session);
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        Query namedQuery = analysis.getNamedQuery(node);
        Scope scope = analysis.getScope(node);

        if (namedQuery != null) {
            RelationPlan subPlan = process(namedQuery, null);

            // Add implicit coercions if view query produces types that don't match the declared output types
            // of the view (e.g., if the underlying tables referenced by the view changed)
            Type[] types = scope.getRelationType().getAllFields().stream().map(Field::getType).toArray(Type[]::new);
            RelationPlan withCoercions = addCoercions(subPlan, types);
            return new RelationPlan(withCoercions.getRoot(), scope, withCoercions.getFieldMappings());
        }

        TableHandle handle = analysis.getTableHandle(node);

        ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columns = ImmutableMap.builder();
        for (Field field : scope.getRelationType().getAllFields()) {
            VariableReferenceExpression variable = variableAllocator.newVariable(getSourceLocation(node), field.getName().get(), field.getType());
            outputVariablesBuilder.add(variable);
            columns.put(variable, analysis.getColumn(field));
        }

        List<VariableReferenceExpression> outputVariables = outputVariablesBuilder.build();
        List<TableConstraint<ColumnHandle>> tableConstraints = metadata.getTableMetadata(session, handle).getMetadata().getTableConstraints();
        PlanNode root = new TableScanNode(getSourceLocation(node.getLocation()), idAllocator.getNextId(), handle, outputVariables, columns.build(), tableConstraints, TupleDomain.all(), TupleDomain.all());

        return new RelationPlan(root, scope, outputVariables);
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        PlanNode root = subPlan.getRoot();
        List<VariableReferenceExpression> mappings = subPlan.getFieldMappings();

        if (node.getColumnNames() != null) {
            ImmutableList.Builder<VariableReferenceExpression> newMappings = ImmutableList.builder();
            Assignments.Builder assignments = Assignments.builder();

            // project only the visible columns from the underlying relation
            for (int i = 0; i < subPlan.getDescriptor().getAllFieldCount(); i++) {
                Field field = subPlan.getDescriptor().getFieldByIndex(i);
                if (!field.isHidden()) {
                    VariableReferenceExpression aliasedColumn = variableAllocator.newVariable(mappings.get(i).getSourceLocation(), field);
                    assignments.put(aliasedColumn, castToRowExpression(asSymbolReference(subPlan.getFieldMappings().get(i))));
                    newMappings.add(aliasedColumn);
                }
            }

            root = new ProjectNode(getSourceLocation(node.getLocation()), idAllocator.getNextId(), subPlan.getRoot(), assignments.build(), LOCAL);
            mappings = newMappings.build();
        }

        return new RelationPlan(root, analysis.getScope(node), mappings);
    }

    @Override
    protected RelationPlan visitSampledRelation(SampledRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        double ratio = analysis.getSampleRatio(node);
        PlanNode planNode = new SampleNode(
                getSourceLocation(node),
                idAllocator.getNextId(),
                subPlan.getRoot(),
                ratio,
                SampleNodeUtil.fromType(node.getType()));
        return new RelationPlan(planNode, analysis.getScope(node), subPlan.getFieldMappings());
    }

    @Override
    protected RelationPlan visitJoin(Join node, Void context)
    {
        // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
        RelationPlan leftPlan = process(node.getLeft(), context);

        Optional<Unnest> unnest = getUnnest(node.getRight());
        if (unnest.isPresent()) {
            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                throw notSupportedException(unnest.get(), "UNNEST on other than the right side of CROSS JOIN");
            }
            return planCrossJoinUnnest(leftPlan, node, unnest.get());
        }

        Optional<Lateral> lateral = getLateral(node.getRight());
        if (lateral.isPresent()) {
            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                throw notSupportedException(lateral.get(), "LATERAL on other than the right side of CROSS JOIN");
            }
            return planLateralJoin(node, leftPlan, lateral.get());
        }

        RelationPlan rightPlan = process(node.getRight(), context);

        if (node.getCriteria().isPresent() && node.getCriteria().get() instanceof JoinUsing) {
            return planJoinUsing(node, leftPlan, rightPlan);
        }

        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

        // NOTE: variables must be in the same order as the outputDescriptor
        List<VariableReferenceExpression> outputs = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(leftPlan.getFieldMappings())
                .addAll(rightPlan.getFieldMappings())
                .build();

        ImmutableList.Builder<JoinNode.EquiJoinClause> equiClauses = ImmutableList.builder();
        List<Expression> complexJoinExpressions = new ArrayList<>();
        List<Expression> postInnerJoinConditions = new ArrayList<>();

        if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
            Expression criteria = analysis.getJoinCriteria(node);

            RelationType left = analysis.getOutputDescriptor(node.getLeft());
            RelationType right = analysis.getOutputDescriptor(node.getRight());

            List<Expression> leftComparisonExpressions = new ArrayList<>();
            List<Expression> rightComparisonExpressions = new ArrayList<>();
            List<ComparisonExpression.Operator> joinConditionComparisonOperators = new ArrayList<>();

            for (Expression conjunct : ExpressionUtils.extractConjuncts(criteria)) {
                conjunct = ExpressionUtils.normalize(conjunct);

                if (!isEqualComparisonExpression(conjunct) && node.getType() != INNER) {
                    complexJoinExpressions.add(conjunct);
                    continue;
                }

                Set<QualifiedName> dependencies = VariablesExtractor.extractNames(conjunct, analysis.getColumnReferences());

                if (dependencies.stream().allMatch(left::canResolve) || dependencies.stream().allMatch(right::canResolve)) {
                    // If the conjunct can be evaluated entirely with the inputs on either side of the join, add
                    // it to the list complex expressions and let the optimizers figure out how to push it down later.
                    complexJoinExpressions.add(conjunct);
                }
                else if (conjunct instanceof ComparisonExpression) {
                    Expression firstExpression = ((ComparisonExpression) conjunct).getLeft();
                    Expression secondExpression = ((ComparisonExpression) conjunct).getRight();
                    ComparisonExpression.Operator comparisonOperator = ((ComparisonExpression) conjunct).getOperator();
                    Set<QualifiedName> firstDependencies = VariablesExtractor.extractNames(firstExpression, analysis.getColumnReferences());
                    Set<QualifiedName> secondDependencies = VariablesExtractor.extractNames(secondExpression, analysis.getColumnReferences());

                    if (firstDependencies.stream().allMatch(left::canResolve) && secondDependencies.stream().allMatch(right::canResolve)) {
                        leftComparisonExpressions.add(firstExpression);
                        rightComparisonExpressions.add(secondExpression);
                        addNullFilters(complexJoinExpressions, node.getType(), firstExpression, secondExpression);
                        joinConditionComparisonOperators.add(comparisonOperator);
                    }
                    else if (firstDependencies.stream().allMatch(right::canResolve) && secondDependencies.stream().allMatch(left::canResolve)) {
                        leftComparisonExpressions.add(secondExpression);
                        rightComparisonExpressions.add(firstExpression);
                        addNullFilters(complexJoinExpressions, node.getType(), secondExpression, firstExpression);
                        joinConditionComparisonOperators.add(comparisonOperator.flip());
                    }
                    else {
                        // the case when we mix variables from both left and right join side on either side of condition.
                        complexJoinExpressions.add(conjunct);
                    }
                }
                else {
                    complexJoinExpressions.add(conjunct);
                }
            }

            leftPlanBuilder = subqueryPlanner.handleSubqueries(leftPlanBuilder, leftComparisonExpressions, node);
            rightPlanBuilder = subqueryPlanner.handleSubqueries(rightPlanBuilder, rightComparisonExpressions, node);

            // Add projections for join criteria
            leftPlanBuilder = leftPlanBuilder.appendProjections(leftComparisonExpressions, variableAllocator, idAllocator);
            rightPlanBuilder = rightPlanBuilder.appendProjections(rightComparisonExpressions, variableAllocator, idAllocator);

            for (int i = 0; i < leftComparisonExpressions.size(); i++) {
                if (joinConditionComparisonOperators.get(i) == ComparisonExpression.Operator.EQUAL) {
                    VariableReferenceExpression leftVariable = leftPlanBuilder.translateToVariable(leftComparisonExpressions.get(i));
                    VariableReferenceExpression rightVariable = rightPlanBuilder.translateToVariable(rightComparisonExpressions.get(i));

                    equiClauses.add(new JoinNode.EquiJoinClause(leftVariable, rightVariable));
                }
                else {
                    Expression leftExpression = leftPlanBuilder.rewrite(leftComparisonExpressions.get(i));
                    Expression rightExpression = rightPlanBuilder.rewrite(rightComparisonExpressions.get(i));
                    postInnerJoinConditions.add(new ComparisonExpression(joinConditionComparisonOperators.get(i), leftExpression, rightExpression));
                }
            }
        }

        PlanNode root = new JoinNode(
                getSourceLocation(node),
                idAllocator.getNextId(),
                JoinNodeUtils.typeConvert(node.getType()),
                leftPlanBuilder.getRoot(),
                rightPlanBuilder.getRoot(),
                equiClauses.build(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(leftPlanBuilder.getRoot().getOutputVariables())
                        .addAll(rightPlanBuilder.getRoot().getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        if (node.getType() != INNER) {
            for (Expression complexExpression : complexJoinExpressions) {
                Set<InPredicate> inPredicates = subqueryPlanner.collectInPredicateSubqueries(complexExpression, node);
                if (!inPredicates.isEmpty()) {
                    InPredicate inPredicate = Iterables.getLast(inPredicates);
                    throw notSupportedException(inPredicate, "IN with subquery predicate in join condition");
                }
            }

            // subqueries can be applied only to one side of join - left side is selected in arbitrary way
            leftPlanBuilder = subqueryPlanner.handleUncorrelatedSubqueries(leftPlanBuilder, complexJoinExpressions, node);
        }

        RelationPlan intermediateRootRelationPlan = new RelationPlan(root, analysis.getScope(node), outputs);
        TranslationMap translationMap = new TranslationMap(intermediateRootRelationPlan, analysis, lambdaDeclarationToVariableMap);
        translationMap.setFieldMappings(outputs);
        translationMap.putExpressionMappingsFrom(leftPlanBuilder.getTranslations());
        translationMap.putExpressionMappingsFrom(rightPlanBuilder.getTranslations());

        if (node.getType() != INNER && !complexJoinExpressions.isEmpty()) {
            Expression joinedFilterCondition = ExpressionUtils.and(complexJoinExpressions);
            Expression rewrittenFilterCondition = translationMap.rewrite(joinedFilterCondition);
            root = new JoinNode(
                    getSourceLocation(node),
                    idAllocator.getNextId(),
                    JoinNodeUtils.typeConvert(node.getType()),
                    leftPlanBuilder.getRoot(),
                    rightPlanBuilder.getRoot(),
                    equiClauses.build(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(leftPlanBuilder.getRoot().getOutputVariables())
                            .addAll(rightPlanBuilder.getRoot().getOutputVariables())
                            .build(),
                    Optional.of(castToRowExpression(rewrittenFilterCondition)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of());
        }

        if (node.getType() == INNER) {
            // rewrite all the other conditions using output variables from left + right plan node.
            PlanBuilder rootPlanBuilder = new PlanBuilder(translationMap, root);
            rootPlanBuilder = subqueryPlanner.handleSubqueries(rootPlanBuilder, complexJoinExpressions, node);

            for (Expression expression : complexJoinExpressions) {
                postInnerJoinConditions.add(rootPlanBuilder.rewrite(expression));
            }
            root = rootPlanBuilder.getRoot();

            Expression postInnerJoinCriteria;
            if (!postInnerJoinConditions.isEmpty()) {
                postInnerJoinCriteria = ExpressionUtils.and(postInnerJoinConditions);
                root = new FilterNode(getSourceLocation(postInnerJoinCriteria), idAllocator.getNextId(), root, castToRowExpression(postInnerJoinCriteria));
            }
        }

        return new RelationPlan(root, analysis.getScope(node), outputs);
    }

    private void addNullFilters(List<Expression> conditions, Join.Type joinType, Expression left, Expression right)
    {
        if (SystemSessionProperties.isOptimizeNullsInJoin(session)) {
            switch (joinType) {
                case INNER:
                    addNullFilterIfSupported(conditions, left);
                    addNullFilterIfSupported(conditions, right);
                    break;
                case LEFT:
                    addNullFilterIfSupported(conditions, right);
                    break;
                case RIGHT:
                    addNullFilterIfSupported(conditions, left);
                    break;
            }
        }
    }

    private void addNullFilterIfSupported(List<Expression> conditions, Expression incoming)
    {
        if (!(incoming instanceof InPredicate)) {
            // (A.x IN (1,2,3)) IS NOT NULL is not supported as a join condition as of today.
            conditions.add(new IsNotNullPredicate(incoming));
        }
    }

    private RelationPlan planJoinUsing(Join node, RelationPlan left, RelationPlan right)
    {
        /* Given: l JOIN r USING (k1, ..., kn)

           produces:

            - project
                    coalesce(l.k1, r.k1)
                    ...,
                    coalesce(l.kn, r.kn)
                    l.v1,
                    ...,
                    l.vn,
                    r.v1,
                    ...,
                    r.vn
              - join (l.k1 = r.k1 and ... l.kn = r.kn)
                    - project
                        cast(l.k1 as commonType(l.k1, r.k1))
                        ...
                    - project
                        cast(rl.k1 as commonType(l.k1, r.k1))

            If casts are redundant (due to column type and common type being equal),
            they will be removed by optimization passes.
        */

        List<Identifier> joinColumns = ((JoinUsing) node.getCriteria().get()).getColumns();

        Analysis.JoinUsingAnalysis joinAnalysis = analysis.getJoinUsing(node);

        ImmutableList.Builder<JoinNode.EquiJoinClause> clauses = ImmutableList.builder();

        Map<Identifier, VariableReferenceExpression> leftJoinColumns = new HashMap<>();
        Map<Identifier, VariableReferenceExpression> rightJoinColumns = new HashMap<>();

        Assignments.Builder leftCoercions = Assignments.builder();
        Assignments.Builder rightCoercions = Assignments.builder();

        leftCoercions.putAll(identitiesAsSymbolReferences(left.getRoot().getOutputVariables()));
        rightCoercions.putAll(identitiesAsSymbolReferences(right.getRoot().getOutputVariables()));
        for (int i = 0; i < joinColumns.size(); i++) {
            Identifier identifier = joinColumns.get(i);
            Type type = analysis.getType(identifier);

            // compute the coercion for the field on the left to the common supertype of left & right
            VariableReferenceExpression leftOutput = variableAllocator.newVariable(identifier, type);
            int leftField = joinAnalysis.getLeftJoinFields().get(i);
            leftCoercions.put(leftOutput, castToRowExpression(new Cast(
                    identifier.getLocation(),
                    createSymbolReference(left.getVariable(leftField)),
                    type.getTypeSignature().toString(),
                    false,
                    metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(left.getDescriptor().getFieldByIndex(leftField).getType(), type))));
            leftJoinColumns.put(identifier, leftOutput);

            // compute the coercion for the field on the right to the common supertype of left & right
            VariableReferenceExpression rightOutput = variableAllocator.newVariable(identifier, type);
            int rightField = joinAnalysis.getRightJoinFields().get(i);
            rightCoercions.put(rightOutput, castToRowExpression(new Cast(
                    identifier.getLocation(),
                    createSymbolReference(right.getVariable(rightField)),
                    type.getTypeSignature().toString(),
                    false,
                    metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(right.getDescriptor().getFieldByIndex(rightField).getType(), type))));
            rightJoinColumns.put(identifier, rightOutput);

            clauses.add(new JoinNode.EquiJoinClause(leftOutput, rightOutput));
        }

        ProjectNode leftCoercion = new ProjectNode(idAllocator.getNextId(), left.getRoot(), leftCoercions.build());
        ProjectNode rightCoercion = new ProjectNode(idAllocator.getNextId(), right.getRoot(), rightCoercions.build());

        JoinNode join = new JoinNode(
                getSourceLocation(node),
                idAllocator.getNextId(),
                JoinNodeUtils.typeConvert(node.getType()),
                leftCoercion,
                rightCoercion,
                clauses.build(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(leftCoercion.getOutputVariables())
                        .addAll(rightCoercion.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        // Add a projection to produce the outputs of the columns in the USING clause,
        // which are defined as coalesce(l.k, r.k)
        Assignments.Builder assignments = Assignments.builder();

        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
        for (Identifier column : joinColumns) {
            VariableReferenceExpression output = variableAllocator.newVariable(column, analysis.getType(column));
            outputs.add(output);
            assignments.put(output, castToRowExpression(new CoalesceExpression(
                    column.getLocation(),
                    createSymbolReference(leftJoinColumns.get(column)),
                    createSymbolReference(rightJoinColumns.get(column)))));
        }

        for (int field : joinAnalysis.getOtherLeftFields()) {
            VariableReferenceExpression variable = left.getFieldMappings().get(field);
            outputs.add(variable);
            assignments.put(variable, castToRowExpression(createSymbolReference(variable)));
        }

        for (int field : joinAnalysis.getOtherRightFields()) {
            VariableReferenceExpression variable = right.getFieldMappings().get(field);
            outputs.add(variable);
            assignments.put(variable, castToRowExpression(createSymbolReference(variable)));
        }

        return new RelationPlan(
                new ProjectNode(idAllocator.getNextId(), join, assignments.build()),
                analysis.getScope(node),
                outputs.build());
    }

    private Optional<Unnest> getUnnest(Relation relation)
    {
        if (relation instanceof AliasedRelation) {
            return getUnnest(((AliasedRelation) relation).getRelation());
        }
        if (relation instanceof Unnest) {
            return Optional.of((Unnest) relation);
        }
        return Optional.empty();
    }

    private Optional<Lateral> getLateral(Relation relation)
    {
        if (relation instanceof AliasedRelation) {
            return getLateral(((AliasedRelation) relation).getRelation());
        }
        if (relation instanceof Lateral) {
            return Optional.of((Lateral) relation);
        }
        return Optional.empty();
    }

    private RelationPlan planLateralJoin(Join join, RelationPlan leftPlan, Lateral lateral)
    {
        RelationPlan rightPlan = process(lateral.getQuery(), null);
        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

        PlanBuilder planBuilder = subqueryPlanner.appendLateralJoin(leftPlanBuilder, rightPlanBuilder, lateral.getQuery(), true, LateralJoinNode.Type.INNER);

        List<VariableReferenceExpression> outputVariables = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(leftPlan.getRoot().getOutputVariables())
                .addAll(rightPlan.getRoot().getOutputVariables())
                .build();
        return new RelationPlan(planBuilder.getRoot(), analysis.getScope(join), outputVariables);
    }

    private RelationPlan planCrossJoinUnnest(RelationPlan leftPlan, Join joinNode, Unnest node)
    {
        RelationType unnestOutputDescriptor = analysis.getOutputDescriptor(node);
        // Create variables for the result of unnesting
        ImmutableList.Builder<VariableReferenceExpression> unnestedVariablesBuilder = ImmutableList.builder();
        for (Field field : unnestOutputDescriptor.getVisibleFields()) {
            VariableReferenceExpression variable = variableAllocator.newVariable(field);
            unnestedVariablesBuilder.add(variable);
        }
        ImmutableList<VariableReferenceExpression> unnestedVariables = unnestedVariablesBuilder.build();

        // Add a projection for all the unnest arguments
        PlanBuilder planBuilder = initializePlanBuilder(leftPlan);
        planBuilder = planBuilder.appendProjections(node.getExpressions(), variableAllocator, idAllocator);
        TranslationMap translations = planBuilder.getTranslations();
        ProjectNode projectNode = (ProjectNode) planBuilder.getRoot();

        ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariables = ImmutableMap.builder();
        UnmodifiableIterator<VariableReferenceExpression> unnestedVariablesIterator = unnestedVariables.iterator();
        for (Expression expression : node.getExpressions()) {
            Type type = analysis.getType(expression);
            VariableReferenceExpression inputVariable = new VariableReferenceExpression(getSourceLocation(expression), translations.get(expression).getName(), type);
            if (type instanceof ArrayType) {
                Type elementType = ((ArrayType) type).getElementType();
                if (!SystemSessionProperties.isLegacyUnnest(session) && elementType instanceof RowType) {
                    ImmutableList.Builder<VariableReferenceExpression> unnestVariableBuilder = ImmutableList.builder();
                    for (int i = 0; i < ((RowType) elementType).getFields().size(); i++) {
                        unnestVariableBuilder.add(unnestedVariablesIterator.next());
                    }
                    unnestVariables.put(inputVariable, unnestVariableBuilder.build());
                }
                else {
                    unnestVariables.put(inputVariable, ImmutableList.of(unnestedVariablesIterator.next()));
                }
            }
            else if (type instanceof MapType) {
                unnestVariables.put(inputVariable, ImmutableList.of(unnestedVariablesIterator.next(), unnestedVariablesIterator.next()));
            }
            else {
                throw new IllegalArgumentException("Unsupported type for UNNEST: " + type);
            }
        }
        Optional<VariableReferenceExpression> ordinalityVariable = node.isWithOrdinality() ? Optional.of(unnestedVariablesIterator.next()) : Optional.empty();
        checkState(!unnestedVariablesIterator.hasNext(), "Not all output variables were matched with input variables");

        UnnestNode unnestNode = new UnnestNode(getSourceLocation(node), idAllocator.getNextId(), projectNode, leftPlan.getFieldMappings(), unnestVariables.build(), ordinalityVariable);
        return new RelationPlan(unnestNode, analysis.getScope(joinNode), unnestNode.getOutputVariables());
    }

    @Override
    protected RelationPlan visitTableSubquery(TableSubquery node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        return new QueryPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session)
                .plan(node);
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        return new QueryPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session)
                .plan(node);
    }

    @Override
    protected RelationPlan visitValues(Values node, Void context)
    {
        Scope scope = analysis.getScope(node);
        ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
        for (Field field : scope.getRelationType().getVisibleFields()) {
            outputVariablesBuilder.add(variableAllocator.newVariable(field));
        }

        ImmutableList.Builder<List<RowExpression>> rowsBuilder = ImmutableList.builder();
        for (Expression row : node.getRows()) {
            ImmutableList.Builder<RowExpression> values = ImmutableList.builder();
            if (row instanceof Row) {
                for (Expression item : ((Row) row).getItems()) {
                    values.add(rewriteRow(item));
                }
            }
            else {
                values.add(rewriteRow(row));
            }
            rowsBuilder.add(values.build());
        }

        ValuesNode valuesNode = new ValuesNode(getSourceLocation(node), idAllocator.getNextId(), outputVariablesBuilder.build(), rowsBuilder.build(), Optional.empty());
        return new RelationPlan(valuesNode, scope, outputVariablesBuilder.build());
    }

    private RowExpression rewriteRow(Expression row)
    {
        // resolve enum literals
        Expression expression = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Type baseType = analysis.getType(node.getBase());
                Type nodeType = analysis.getType(node);
                if (isEnumType(baseType) && isEnumType(nodeType)) {
                    return new EnumLiteral(nodeType.getTypeSignature().toString(), resolveEnumLiteral(node, nodeType));
                }
                return node;
            }
        }, row);
        expression = Coercer.addCoercions(expression, analysis);
        expression = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis), expression);
        return castToRowExpression(expression);
    }

    @Override
    protected RelationPlan visitUnnest(Unnest node, Void context)
    {
        Scope scope = analysis.getScope(node);
        ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
        for (Field field : scope.getRelationType().getVisibleFields()) {
            VariableReferenceExpression variable = variableAllocator.newVariable(field);
            outputVariablesBuilder.add(variable);
        }
        List<VariableReferenceExpression> unnestedVariables = outputVariablesBuilder.build();

        // If we got here, then we must be unnesting a constant, and not be in a join (where there could be column references)
        ImmutableList.Builder<VariableReferenceExpression> argumentVariables = ImmutableList.builder();
        ImmutableList.Builder<RowExpression> values = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariables = ImmutableMap.builder();
        Iterator<VariableReferenceExpression> unnestedVariablesIterator = unnestedVariables.iterator();
        for (Expression expression : node.getExpressions()) {
            Type type = analysis.getType(expression);
            Expression rewritten = Coercer.addCoercions(expression, analysis);
            rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis), rewritten);
            values.add(castToRowExpression(rewritten));
            VariableReferenceExpression input = variableAllocator.newVariable(rewritten, type);
            argumentVariables.add(new VariableReferenceExpression(getSourceLocation(rewritten), input.getName(), type));
            if (type instanceof ArrayType) {
                Type elementType = ((ArrayType) type).getElementType();
                if (!SystemSessionProperties.isLegacyUnnest(session) && elementType instanceof RowType) {
                    ImmutableList.Builder<VariableReferenceExpression> unnestVariableBuilder = ImmutableList.builder();
                    for (int i = 0; i < ((RowType) elementType).getFields().size(); i++) {
                        unnestVariableBuilder.add(unnestedVariablesIterator.next());
                    }
                    unnestVariables.put(input, unnestVariableBuilder.build());
                }
                else {
                    unnestVariables.put(input, ImmutableList.of(unnestedVariablesIterator.next()));
                }
            }
            else if (type instanceof MapType) {
                unnestVariables.put(input, ImmutableList.of(unnestedVariablesIterator.next(), unnestedVariablesIterator.next()));
            }
            else {
                throw new IllegalArgumentException("Unsupported type for UNNEST: " + type);
            }
        }
        Optional<VariableReferenceExpression> ordinalityVariable = node.isWithOrdinality() ? Optional.of(unnestedVariablesIterator.next()) : Optional.empty();
        checkState(!unnestedVariablesIterator.hasNext(), "Not all output variables were matched with input variables");
        ValuesNode valuesNode = new ValuesNode(
                getSourceLocation(node),
                idAllocator.getNextId(),
                argumentVariables.build(),
                ImmutableList.of(values.build()),
                Optional.empty());

        UnnestNode unnestNode = new UnnestNode(getSourceLocation(node), idAllocator.getNextId(), valuesNode, ImmutableList.of(), unnestVariables.build(), ordinalityVariable);
        return new RelationPlan(unnestNode, scope, unnestedVariables);
    }

    private RelationPlan processAndCoerceIfNecessary(Relation node, Void context)
    {
        Type[] coerceToTypes = analysis.getRelationCoercion(node);

        RelationPlan plan = this.process(node, context);

        if (coerceToTypes == null) {
            return plan;
        }

        return addCoercions(plan, coerceToTypes);
    }

    private RelationPlan addCoercions(RelationPlan plan, Type[] targetColumnTypes)
    {
        RelationType oldRelation = plan.getDescriptor();
        List<VariableReferenceExpression> oldVisibleVariables = oldRelation.getVisibleFields().stream()
                .map(oldRelation::indexOf)
                .map(plan.getFieldMappings()::get)
                .collect(toImmutableList());
        RelationType oldRelationWithVisibleFields = plan.getDescriptor().withOnlyVisibleFields();
        verify(targetColumnTypes.length == oldVisibleVariables.size());
        ImmutableList.Builder<VariableReferenceExpression> newVariables = new ImmutableList.Builder<>();
        Field[] newFields = new Field[targetColumnTypes.length];
        Assignments.Builder assignments = Assignments.builder();
        for (int i = 0; i < targetColumnTypes.length; i++) {
            VariableReferenceExpression inputVariable = oldVisibleVariables.get(i);
            Field oldField = oldRelationWithVisibleFields.getFieldByIndex(i);
            Type outputType = targetColumnTypes[i];
            if (!outputType.equals(inputVariable.getType())) {
                Expression cast = new Cast(createSymbolReference(inputVariable), outputType.getTypeSignature().toString());
                VariableReferenceExpression outputVariable = variableAllocator.newVariable(cast, outputType);
                assignments.put(outputVariable, castToRowExpression(cast));
                newVariables.add(outputVariable);
            }
            else {
                SymbolReference symbolReference = new SymbolReference(oldField.getNodeLocation(), inputVariable.getName());
                VariableReferenceExpression outputVariable = variableAllocator.newVariable(symbolReference, outputType);
                assignments.put(outputVariable, castToRowExpression(symbolReference));
                newVariables.add(outputVariable);
            }
            newFields[i] = new Field(
                    oldField.getNodeLocation(),
                    oldField.getRelationAlias(),
                    oldField.getName(),
                    targetColumnTypes[i],
                    oldField.isHidden(),
                    oldField.getOriginTable(),
                    oldField.getOriginColumnName(),
                    oldField.isAliased());
        }
        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());
        return new RelationPlan(projectNode, Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(newFields)).build(), newVariables.build());
    }

    @Override
    protected RelationPlan visitUnion(Union node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new UnionNode(getSourceLocation(node), idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getOutputVariables(), setOperationPlan.getVariableMapping());
        if (node.isDistinct().orElse(true)) {
            planNode = distinct(planNode);
        }
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputVariables());
    }

    @Override
    protected RelationPlan visitIntersect(Intersect node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for INTERSECT");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new IntersectNode(getSourceLocation(node), idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getOutputVariables(), setOperationPlan.getVariableMapping());
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputVariables());
    }

    @Override
    protected RelationPlan visitExcept(Except node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for EXCEPT");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new ExceptNode(getSourceLocation(node), idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getOutputVariables(), setOperationPlan.getVariableMapping());
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputVariables());
    }

    private SetOperationPlan process(SetOperation node)
    {
        List<VariableReferenceExpression> outputs = null;
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> variableMapping = ImmutableListMultimap.builder();

        List<RelationPlan> subPlans = node.getRelations().stream()
                .map(relation -> processAndCoerceIfNecessary(relation, null))
                .collect(toImmutableList());

        for (RelationPlan relationPlan : subPlans) {
            List<VariableReferenceExpression> childOutputVariables = relationPlan.getFieldMappings();
            if (outputs == null) {
                // Use the first Relation to derive output variable names
                RelationType descriptor = relationPlan.getDescriptor();
                ImmutableList.Builder<VariableReferenceExpression> outputVariableBuilder = ImmutableList.builder();
                for (Field field : descriptor.getVisibleFields()) {
                    int fieldIndex = descriptor.indexOf(field);
                    VariableReferenceExpression variable = childOutputVariables.get(fieldIndex);
                    outputVariableBuilder.add(variableAllocator.newVariable(variable));
                }
                outputs = outputVariableBuilder.build();
            }

            RelationType descriptor = relationPlan.getDescriptor();
            checkArgument(descriptor.getVisibleFieldCount() == outputs.size(),
                    "Expected relation to have %s variables but has %s variables",
                    descriptor.getVisibleFieldCount(),
                    outputs.size());

            int fieldId = 0;
            for (Field field : descriptor.getVisibleFields()) {
                int fieldIndex = descriptor.indexOf(field);
                variableMapping.put(outputs.get(fieldId), childOutputVariables.get(fieldIndex));
                fieldId++;
            }

            sources.add(relationPlan.getRoot());
        }

        return new SetOperationPlan(sources.build(), variableMapping.build());
    }

    private PlanBuilder initializePlanBuilder(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToVariableMap);

        // Make field->variable mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        return new PlanBuilder(translations, relationPlan.getRoot());
    }

    private PlanNode distinct(PlanNode node)
    {
        return new AggregationNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node,
                ImmutableMap.of(),
                singleGroupingSet(node.getOutputVariables()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
    }

    private static class SetOperationPlan
    {
        private final List<PlanNode> sources;
        private final List<VariableReferenceExpression> outputVariables;
        private final Map<VariableReferenceExpression, List<VariableReferenceExpression>> variableMapping;

        private SetOperationPlan(List<PlanNode> sources, ListMultimap<VariableReferenceExpression, VariableReferenceExpression> variableMapping)
        {
            this.sources = sources;
            this.outputVariables = ImmutableList.copyOf(variableMapping.keySet());
            Map<VariableReferenceExpression, List<VariableReferenceExpression>> mapping = new LinkedHashMap<>();
            variableMapping.asMap().forEach((key, value) -> {
                checkState(value instanceof List, "variableMapping values should be of type List");
                mapping.put(key, (List<VariableReferenceExpression>) value);
            });
            this.variableMapping = mapping;
        }

        public List<PlanNode> getSources()
        {
            return sources;
        }

        public List<VariableReferenceExpression> getOutputVariables()
        {
            return outputVariables;
        }

        public Map<VariableReferenceExpression, List<VariableReferenceExpression>> getVariableMapping()
        {
            return variableMapping;
        }
    }
}
