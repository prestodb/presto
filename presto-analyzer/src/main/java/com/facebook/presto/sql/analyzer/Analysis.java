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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.AccessControlInfo;
import com.facebook.presto.spi.analyzer.AccessControlInfoForTable;
import com.facebook.presto.spi.analyzer.AccessControlReferences;
import com.facebook.presto.spi.analyzer.AccessControlRole;
import com.facebook.presto.spi.analyzer.UpdateInfo;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Offset;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableFunctionInvocation;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.Analysis.MaterializedViewAnalysisState.NOT_VISITED;
import static com.facebook.presto.sql.analyzer.Analysis.MaterializedViewAnalysisState.VISITED;
import static com.facebook.presto.sql.analyzer.Analysis.MaterializedViewAnalysisState.VISITING;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Multimaps.forMap;
import static com.google.common.collect.Multimaps.unmodifiableMultimap;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class Analysis
{
    @Nullable
    private final Statement root;
    private final Map<NodeRef<Parameter>, Expression> parameters;
    private UpdateInfo updateInfo;

    private final Map<NodeRef<Table>, NamedQuery> namedQueries = new LinkedHashMap<>();

    private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();
    private final Multimap<NodeRef<Expression>, FieldId> columnReferences = ArrayListMultimap.create();

    private final AccessControlReferences accessControlReferences = new AccessControlReferences();

    // a map of users to the columns per table that they access
    private final Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> tableColumnReferences = new LinkedHashMap<>();
    private final Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> utilizedTableColumnReferences = new LinkedHashMap<>();
    private final Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> tableColumnAndSubfieldReferences = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, List<FunctionCall>> aggregates = new LinkedHashMap<>();
    private final Map<NodeRef<OrderBy>, List<Expression>> orderByAggregates = new LinkedHashMap<>();
    private final Map<NodeRef<QuerySpecification>, List<Expression>> groupByExpressions = new LinkedHashMap<>();
    private final Map<NodeRef<QuerySpecification>, GroupingSetAnalysis> groupingSets = new LinkedHashMap<>();

    private final Map<NodeRef<Node>, Expression> where = new LinkedHashMap<>();
    private final Map<NodeRef<QuerySpecification>, Expression> having = new LinkedHashMap<>();
    private final Map<NodeRef<Node>, List<Expression>> orderByExpressions = new LinkedHashMap<>();
    private final Set<NodeRef<OrderBy>> redundantOrderBy = new HashSet<>();
    private final Map<NodeRef<Node>, List<Expression>> outputExpressions = new LinkedHashMap<>();
    private final Map<NodeRef<QuerySpecification>, List<FunctionCall>> windowFunctions = new LinkedHashMap<>();
    private final Map<NodeRef<OrderBy>, List<FunctionCall>> orderByWindowFunctions = new LinkedHashMap<>();
    private final Map<NodeRef<Offset>, Long> offset = new LinkedHashMap<>();

    private final Map<NodeRef<Join>, Expression> joins = new LinkedHashMap<>();
    private final Map<NodeRef<Join>, JoinUsingAnalysis> joinUsing = new LinkedHashMap<>();

    private final ListMultimap<NodeRef<Node>, InPredicate> inPredicatesSubqueries = ArrayListMultimap.create();
    private final ListMultimap<NodeRef<Node>, SubqueryExpression> scalarSubqueries = ArrayListMultimap.create();
    private final ListMultimap<NodeRef<Node>, ExistsPredicate> existsSubqueries = ArrayListMultimap.create();
    private final ListMultimap<NodeRef<Node>, QuantifiedComparisonExpression> quantifiedComparisonSubqueries = ArrayListMultimap.create();

    private final MetadataHandle metadataHandle = new MetadataHandle();
    private final Map<NodeRef<Table>, TableHandle> tables = new LinkedHashMap<>();

    private final Map<NodeRef<Expression>, Type> types = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, Type> coercions = new LinkedHashMap<>();
    // Coercions needed for window function frame of type RANGE.
    // These are coercions for the sort key, needed for frame bound calculation, identified by frame range offset expression.
    // Frame definition might contain two different offset expressions (for start and end), each requiring different coercion of the sort key.
    private final Map<NodeRef<Expression>, Type> sortKeyCoercionsForFrameBoundCalculation = new LinkedHashMap<>();
    // Coercions needed for window function frame of type RANGE.
    // These are coercions for the sort key, needed for comparison of the sort key with precomputed frame bound, identified by frame range offset expression.
    private final Map<NodeRef<Expression>, Type> sortKeyCoercionsForFrameBoundComparison = new LinkedHashMap<>();
    // Functions for calculating frame bounds for frame of type RANGE, identified by frame range offset expression.
    private final Map<NodeRef<Expression>, FunctionHandle> frameBoundCalculations = new LinkedHashMap<>();
    private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();
    private final Map<NodeRef<Relation>, List<Type>> relationCoercions = new LinkedHashMap<>();
    private final Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles = new LinkedHashMap<>();
    private final Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences = new LinkedHashMap<>();

    private final Map<Field, ColumnHandle> columns = new LinkedHashMap<>();

    private final Map<NodeRef<SampledRelation>, Double> sampleRatios = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, List<GroupingOperation>> groupingOperations = new LinkedHashMap<>();

    private final Multiset<RowFilterScopeEntry> rowFilterScopes = HashMultiset.create();
    private final Map<NodeRef<Table>, List<Expression>> rowFilters = new LinkedHashMap<>();

    private final Multiset<ColumnMaskScopeEntry> columnMaskScopes = HashMultiset.create();
    private final Map<NodeRef<Table>, Map<String, Expression>> columnMasks = new LinkedHashMap<>();

    // for create table
    private Optional<QualifiedObjectName> createTableDestination = Optional.empty();
    private Map<String, Expression> createTableProperties = ImmutableMap.of();
    private boolean createTableAsSelectWithData = true;
    private boolean createTableAsSelectNoOp;
    private Optional<List<Identifier>> createTableColumnAliases = Optional.empty();
    private Optional<String> createTableComment = Optional.empty();

    private Optional<Insert> insert = Optional.empty();
    private Optional<RefreshMaterializedViewAnalysis> refreshMaterializedViewAnalysis = Optional.empty();
    private Optional<TableHandle> analyzeTarget = Optional.empty();

    private Optional<List<ColumnMetadata>> updatedColumns = Optional.empty();

    // for describe input and describe output
    private final boolean isDescribe;

    // for recursive view detection
    private final Deque<Table> tablesForView = new ArrayDeque<>();

    // To prevent recursive analyzing of one materialized view base table
    private final ListMultimap<NodeRef<Table>, Table> tablesForMaterializedView = ArrayListMultimap.create();

    // for materialized view analysis state detection, state is used to identify if materialized view has been expanded or in-process.
    private final Map<Table, MaterializedViewAnalysisState> materializedViewAnalysisStateMap = new HashMap<>();

    private final Map<QualifiedObjectName, String> materializedViews = new LinkedHashMap<>();

    private Optional<String> expandedQuery = Optional.empty();

    // Keeps track of the subquery we are visiting, so we have access to base query information when processing materialized view status
    private Optional<QuerySpecification> currentQuerySpecification = Optional.empty();

    // names of tables and aliased relations. All names are resolved case-insensitive.
    private final Map<NodeRef<Relation>, QualifiedName> relationNames = new LinkedHashMap<>();
    private final Map<NodeRef<TableFunctionInvocation>, TableFunctionInvocationAnalysis> tableFunctionAnalyses = new LinkedHashMap<>();
    private final Set<NodeRef<Relation>> aliasedRelations = new LinkedHashSet<>();
    private final Set<NodeRef<TableFunctionInvocation>> polymorphicTableFunctions = new LinkedHashSet<>();

    public Analysis(@Nullable Statement root, Map<NodeRef<Parameter>, Expression> parameters, boolean isDescribe)
    {
        this.root = root;
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameterMap is null"));
        this.isDescribe = isDescribe;
    }

    public Statement getStatement()
    {
        return root;
    }

    public UpdateInfo getUpdateInfo()
    {
        return updateInfo;
    }

    public void setUpdateInfo(UpdateInfo updateInfo)
    {
        this.updateInfo = updateInfo;
    }

    public boolean isCreateTableAsSelectWithData()
    {
        return createTableAsSelectWithData;
    }

    public void setCreateTableAsSelectWithData(boolean createTableAsSelectWithData)
    {
        this.createTableAsSelectWithData = createTableAsSelectWithData;
    }

    public boolean isCreateTableAsSelectNoOp()
    {
        return createTableAsSelectNoOp;
    }

    public void setCreateTableAsSelectNoOp(boolean createTableAsSelectNoOp)
    {
        this.createTableAsSelectNoOp = createTableAsSelectNoOp;
    }

    public void setAggregates(QuerySpecification node, List<FunctionCall> aggregates)
    {
        this.aggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
    }

    public List<FunctionCall> getAggregates(QuerySpecification query)
    {
        return aggregates.get(NodeRef.of(query));
    }

    public void setOrderByAggregates(OrderBy node, List<Expression> aggregates)
    {
        this.orderByAggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
    }

    public List<Expression> getOrderByAggregates(OrderBy node)
    {
        return orderByAggregates.get(NodeRef.of(node));
    }

    public Map<NodeRef<Expression>, Type> getTypes()
    {
        return unmodifiableMap(types);
    }

    public Type getType(Expression expression)
    {
        Type type = types.get(NodeRef.of(expression));
        checkArgument(type != null, "Expression not analyzed: %s", expression);
        return type;
    }

    public Type getTypeWithCoercions(Expression expression)
    {
        NodeRef<Expression> key = NodeRef.of(expression);
        checkArgument(types.containsKey(key), "Expression not analyzed: %s", expression);
        if (coercions.containsKey(key)) {
            return coercions.get(key);
        }
        return types.get(key);
    }

    public Type[] getRelationCoercion(Relation relation)
    {
        return Optional.ofNullable(relationCoercions.get(NodeRef.of(relation)))
                .map(types -> types.stream().toArray(Type[]::new))
                .orElse(null);
    }

    public void addRelationCoercion(Relation relation, Type[] types)
    {
        relationCoercions.put(NodeRef.of(relation), ImmutableList.copyOf(types));
    }

    public Map<NodeRef<Expression>, Type> getCoercions()
    {
        return unmodifiableMap(coercions);
    }

    public Set<NodeRef<Expression>> getTypeOnlyCoercions()
    {
        return unmodifiableSet(typeOnlyCoercions);
    }

    public Type getCoercion(Expression expression)
    {
        return coercions.get(NodeRef.of(expression));
    }

    public void addLambdaArgumentReferences(Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences)
    {
        this.lambdaArgumentReferences.putAll(lambdaArgumentReferences);
    }

    public LambdaArgumentDeclaration getLambdaArgumentReference(Identifier identifier)
    {
        return lambdaArgumentReferences.get(NodeRef.of(identifier));
    }

    public Map<NodeRef<Identifier>, LambdaArgumentDeclaration> getLambdaArgumentReferences()
    {
        return unmodifiableMap(lambdaArgumentReferences);
    }

    public void setGroupingSets(QuerySpecification node, GroupingSetAnalysis groupingSets)
    {
        this.groupingSets.put(NodeRef.of(node), groupingSets);
    }

    public void setGroupByExpressions(QuerySpecification node, List<Expression> expressions)
    {
        groupByExpressions.put(NodeRef.of(node), expressions);
    }

    public boolean isAggregation(QuerySpecification node)
    {
        return groupByExpressions.containsKey(NodeRef.of(node));
    }

    public boolean isTypeOnlyCoercion(Expression expression)
    {
        return typeOnlyCoercions.contains(NodeRef.of(expression));
    }

    public GroupingSetAnalysis getGroupingSets(QuerySpecification node)
    {
        return groupingSets.get(NodeRef.of(node));
    }

    public List<Expression> getGroupByExpressions(QuerySpecification node)
    {
        return groupByExpressions.get(NodeRef.of(node));
    }

    public void setWhere(Node node, Expression expression)
    {
        where.put(NodeRef.of(node), expression);
    }

    public Expression getWhere(QuerySpecification node)
    {
        return where.get(NodeRef.<Node>of(node));
    }

    public void setOrderByExpressions(Node node, List<Expression> items)
    {
        orderByExpressions.put(NodeRef.of(node), ImmutableList.copyOf(items));
    }

    public List<Expression> getOrderByExpressions(Node node)
    {
        return orderByExpressions.get(NodeRef.of(node));
    }

    public void setOffset(Offset node, long rowCount)
    {
        offset.put(NodeRef.of(node), rowCount);
    }

    public long getOffset(Offset node)
    {
        checkState(offset.containsKey(NodeRef.of(node)), "missing OFFSET value for node %s", node);
        return offset.get(NodeRef.of(node));
    }

    public void setOutputExpressions(Node node, List<Expression> expressions)
    {
        outputExpressions.put(NodeRef.of(node), ImmutableList.copyOf(expressions));
    }

    public List<Expression> getOutputExpressions(Node node)
    {
        return outputExpressions.get(NodeRef.of(node));
    }

    public void setHaving(QuerySpecification node, Expression expression)
    {
        having.put(NodeRef.of(node), expression);
    }

    public void setJoinCriteria(Join node, Expression criteria)
    {
        joins.put(NodeRef.of(node), criteria);
    }

    public Expression getJoinCriteria(Join join)
    {
        return joins.get(NodeRef.of(join));
    }

    public void recordSubqueries(Node node, ExpressionAnalysis expressionAnalysis)
    {
        NodeRef<Node> key = NodeRef.of(node);
        this.inPredicatesSubqueries.putAll(key, dereference(expressionAnalysis.getSubqueryInPredicates()));
        this.scalarSubqueries.putAll(key, dereference(expressionAnalysis.getScalarSubqueries()));
        this.existsSubqueries.putAll(key, dereference(expressionAnalysis.getExistsSubqueries()));
        this.quantifiedComparisonSubqueries.putAll(key, dereference(expressionAnalysis.getQuantifiedComparisons()));
    }

    private <T extends Node> List<T> dereference(Collection<NodeRef<T>> nodeRefs)
    {
        return nodeRefs.stream()
                .map(NodeRef::getNode)
                .collect(toImmutableList());
    }

    public List<InPredicate> getInPredicateSubqueries(Node node)
    {
        return ImmutableList.copyOf(inPredicatesSubqueries.get(NodeRef.of(node)));
    }

    public List<SubqueryExpression> getScalarSubqueries(Node node)
    {
        return ImmutableList.copyOf(scalarSubqueries.get(NodeRef.of(node)));
    }

    public boolean isScalarSubquery(SubqueryExpression subqueryExpression)
    {
        return scalarSubqueries.values().contains(subqueryExpression);
    }

    public List<ExistsPredicate> getExistsSubqueries(Node node)
    {
        return ImmutableList.copyOf(existsSubqueries.get(NodeRef.of(node)));
    }

    public List<QuantifiedComparisonExpression> getQuantifiedComparisonSubqueries(Node node)
    {
        return unmodifiableList(quantifiedComparisonSubqueries.get(NodeRef.of(node)));
    }

    public void setWindowFunctions(QuerySpecification node, List<FunctionCall> functions)
    {
        windowFunctions.put(NodeRef.of(node), ImmutableList.copyOf(functions));
    }

    public List<FunctionCall> getWindowFunctions(QuerySpecification query)
    {
        return windowFunctions.get(NodeRef.of(query));
    }

    public void setOrderByWindowFunctions(OrderBy node, List<FunctionCall> functions)
    {
        orderByWindowFunctions.put(NodeRef.of(node), ImmutableList.copyOf(functions));
    }

    public List<FunctionCall> getOrderByWindowFunctions(OrderBy query)
    {
        return orderByWindowFunctions.get(NodeRef.of(query));
    }

    public void addColumnReferences(Map<NodeRef<Expression>, FieldId> columnReferences)
    {
        this.columnReferences.putAll(forMap(columnReferences));
    }

    public void addColumnReference(NodeRef<Expression> node, FieldId fieldId)
    {
        this.columnReferences.put(node, fieldId);
    }

    public Scope getScope(Node node)
    {
        return tryGetScope(node).orElseThrow(() -> new IllegalArgumentException(format("Analysis does not contain information for node: %s", node)));
    }

    public Optional<Scope> tryGetScope(Node node)
    {
        NodeRef<Node> key = NodeRef.of(node);
        if (scopes.containsKey(key)) {
            return Optional.of(scopes.get(key));
        }

        return Optional.empty();
    }

    public Scope getRootScope()
    {
        return getScope(root);
    }

    public void setScope(Node node, Scope scope)
    {
        scopes.put(NodeRef.of(node), scope);
    }

    public RelationType getOutputDescriptor()
    {
        return getOutputDescriptor(root);
    }

    public RelationType getOutputDescriptor(Node node)
    {
        return getScope(node).getRelationType();
    }

    public MetadataHandle getMetadataHandle()
    {
        return metadataHandle;
    }

    public TableHandle getTableHandle(Table table)
    {
        return tables.get(NodeRef.of(table));
    }

    public Collection<TableHandle> getTables()
    {
        return unmodifiableCollection(tables.values());
    }

    public List<Table> getTableNodes()
    {
        return tables.keySet().stream().map(NodeRef::getNode).collect(toImmutableList());
    }

    public void registerTable(Table table, TableHandle handle)
    {
        tables.put(NodeRef.of(table), handle);
    }

    public FunctionHandle getFunctionHandle(FunctionCall function)
    {
        return functionHandles.get(NodeRef.of(function));
    }

    public Map<NodeRef<FunctionCall>, FunctionHandle> getFunctionHandles()
    {
        return ImmutableMap.copyOf(functionHandles);
    }

    public void addFunctionHandles(Map<NodeRef<FunctionCall>, FunctionHandle> infos)
    {
        functionHandles.putAll(infos);
    }

    public Set<NodeRef<Expression>> getColumnReferences()
    {
        return unmodifiableSet(columnReferences.keySet());
    }

    public Multimap<NodeRef<Expression>, FieldId> getColumnReferenceFields()
    {
        return unmodifiableMultimap(columnReferences);
    }

    public boolean isColumnReference(Expression expression)
    {
        requireNonNull(expression, "expression is null");
        checkArgument(getType(expression) != null, "expression %s has not been analyzed", expression);
        return columnReferences.containsKey(NodeRef.of(expression));
    }

    public void addTypes(Map<NodeRef<Expression>, Type> types)
    {
        this.types.putAll(types);
    }

    public void addCoercion(Expression expression, Type type, boolean isTypeOnlyCoercion)
    {
        this.coercions.put(NodeRef.of(expression), type);
        if (isTypeOnlyCoercion) {
            this.typeOnlyCoercions.add(NodeRef.of(expression));
        }
    }

    public void addCoercions(
            Map<NodeRef<Expression>, Type> coercions,
            Set<NodeRef<Expression>> typeOnlyCoercions,
            Map<NodeRef<Expression>, Type> sortKeyCoercionsForFrameBoundCalculation,
            Map<NodeRef<Expression>, Type> sortKeyCoercionsForFrameBoundComparison)
    {
        this.coercions.putAll(coercions);
        this.typeOnlyCoercions.addAll(typeOnlyCoercions);
        this.sortKeyCoercionsForFrameBoundCalculation.putAll(sortKeyCoercionsForFrameBoundCalculation);
        this.sortKeyCoercionsForFrameBoundComparison.putAll(sortKeyCoercionsForFrameBoundComparison);
    }

    public Type getSortKeyCoercionForFrameBoundCalculation(Expression frameOffset)
    {
        return sortKeyCoercionsForFrameBoundCalculation.get(NodeRef.of(frameOffset));
    }

    public Type getSortKeyCoercionForFrameBoundComparison(Expression frameOffset)
    {
        return sortKeyCoercionsForFrameBoundComparison.get(NodeRef.of(frameOffset));
    }

    public void addFrameBoundCalculations(Map<NodeRef<Expression>, FunctionHandle> frameBoundCalculations)
    {
        this.frameBoundCalculations.putAll(frameBoundCalculations);
    }

    public FunctionHandle getFrameBoundCalculation(Expression frameOffset)
    {
        return frameBoundCalculations.get(NodeRef.of(frameOffset));
    }

    public Expression getHaving(QuerySpecification query)
    {
        return having.get(NodeRef.of(query));
    }

    public void setColumn(Field field, ColumnHandle handle)
    {
        columns.put(field, handle);
    }

    public ColumnHandle getColumn(Field field)
    {
        return columns.get(field);
    }

    public void setCreateTableDestination(QualifiedObjectName destination)
    {
        this.createTableDestination = Optional.of(destination);
    }

    public Optional<QualifiedObjectName> getCreateTableDestination()
    {
        return createTableDestination;
    }

    public Optional<TableHandle> getAnalyzeTarget()
    {
        return analyzeTarget;
    }

    public void setAnalyzeTarget(TableHandle analyzeTarget)
    {
        this.analyzeTarget = Optional.of(analyzeTarget);
    }

    public void setCreateTableProperties(Map<String, Expression> createTableProperties)
    {
        this.createTableProperties = ImmutableMap.copyOf(createTableProperties);
    }

    public Map<String, Expression> getCreateTableProperties()
    {
        return createTableProperties;
    }

    public Optional<List<Identifier>> getColumnAliases()
    {
        return createTableColumnAliases;
    }

    public void setCreateTableColumnAliases(List<Identifier> createTableColumnAliases)
    {
        this.createTableColumnAliases = Optional.of(createTableColumnAliases);
    }

    public void setCreateTableComment(Optional<String> createTableComment)
    {
        this.createTableComment = requireNonNull(createTableComment);
    }

    public Optional<String> getCreateTableComment()
    {
        return createTableComment;
    }

    public void setInsert(Insert insert)
    {
        this.insert = Optional.of(insert);
    }

    public Optional<Insert> getInsert()
    {
        return insert;
    }

    public void setUpdatedColumns(List<ColumnMetadata> updatedColumns)
    {
        this.updatedColumns = Optional.of(updatedColumns);
    }

    public Optional<List<ColumnMetadata>> getUpdatedColumns()
    {
        return updatedColumns;
    }

    public void setRefreshMaterializedViewAnalysis(RefreshMaterializedViewAnalysis refreshMaterializedViewAnalysis)
    {
        this.refreshMaterializedViewAnalysis = Optional.of(refreshMaterializedViewAnalysis);
    }

    public Optional<RefreshMaterializedViewAnalysis> getRefreshMaterializedViewAnalysis()
    {
        return refreshMaterializedViewAnalysis;
    }

    public NamedQuery getNamedQuery(Table table)
    {
        return namedQueries.get(NodeRef.of(table));
    }

    public void registerNamedQuery(Table tableReference, Query query, boolean isFromView)
    {
        requireNonNull(tableReference, "tableReference is null");
        requireNonNull(query, "query is null");

        namedQueries.put(NodeRef.of(tableReference), new NamedQuery(query, isFromView));
    }

    public void registerTableForView(Table tableReference)
    {
        tablesForView.push(requireNonNull(tableReference, "table is null"));
    }

    public void unregisterTableForView()
    {
        tablesForView.pop();
    }

    public void registerMaterializedViewForAnalysis(QualifiedObjectName materializedViewName, Table materializedView, String materializedViewSql)
    {
        requireNonNull(materializedView, "materializedView is null");
        if (materializedViewAnalysisStateMap.containsKey(materializedView)) {
            materializedViewAnalysisStateMap.put(materializedView, VISITED);
        }
        else {
            materializedViewAnalysisStateMap.put(materializedView, VISITING);
        }

        materializedViews.put(materializedViewName, materializedViewSql);
    }

    public void unregisterMaterializedViewForAnalysis(Table materializedView)
    {
        requireNonNull(materializedView, "materializedView is null");
        checkState(
                materializedViewAnalysisStateMap.containsKey(materializedView),
                format("materializedViewAnalysisStateMap does not contain materialized view : %s", materializedView.getName()));
        materializedViewAnalysisStateMap.remove(materializedView);
    }

    public MaterializedViewAnalysisState getMaterializedViewAnalysisState(Table materializedView)
    {
        requireNonNull(materializedView, "materializedView is null");
        return materializedViewAnalysisStateMap.getOrDefault(materializedView, NOT_VISITED);
    }

    public boolean hasTableInView(Table tableReference)
    {
        return tablesForView.contains(tableReference);
    }

    public void registerTableForMaterializedView(Table view, Table table)
    {
        requireNonNull(view, "view is null");
        requireNonNull(table, "table is null");

        tablesForMaterializedView.put(NodeRef.of(view), table);
    }

    public void unregisterTableForMaterializedView(Table view, Table table)
    {
        requireNonNull(view, "view is null");
        requireNonNull(table, "table is null");

        tablesForMaterializedView.remove(NodeRef.of(view), table);
    }

    public boolean hasTableRegisteredForMaterializedView(Table view, Table table)
    {
        requireNonNull(view, "view is null");
        requireNonNull(table, "table is null");

        return tablesForMaterializedView.containsEntry(NodeRef.of(view), table);
    }

    public void setSampleRatio(SampledRelation relation, double ratio)
    {
        sampleRatios.put(NodeRef.of(relation), ratio);
    }

    public double getSampleRatio(SampledRelation relation)
    {
        NodeRef<SampledRelation> key = NodeRef.of(relation);
        checkState(sampleRatios.containsKey(key), "Sample ratio missing for %s. Broken analysis?", relation);
        return sampleRatios.get(key);
    }

    public void setGroupingOperations(QuerySpecification querySpecification, List<GroupingOperation> groupingOperations)
    {
        this.groupingOperations.put(NodeRef.of(querySpecification), ImmutableList.copyOf(groupingOperations));
    }

    public List<GroupingOperation> getGroupingOperations(QuerySpecification querySpecification)
    {
        return Optional.ofNullable(groupingOperations.get(NodeRef.of(querySpecification)))
                .orElse(emptyList());
    }

    public Map<NodeRef<Parameter>, Expression> getParameters()
    {
        return parameters;
    }

    public boolean isDescribe()
    {
        return isDescribe;
    }

    public void setJoinUsing(Join node, JoinUsingAnalysis analysis)
    {
        joinUsing.put(NodeRef.of(node), analysis);
    }

    public JoinUsingAnalysis getJoinUsing(Join node)
    {
        return joinUsing.get(NodeRef.of(node));
    }

    public AccessControlReferences getAccessControlReferences()
    {
        return accessControlReferences;
    }

    public void addQueryAccessControlInfo(AccessControlInfo accessControlInfo)
    {
        accessControlReferences.setQueryAccessControlInfo(accessControlInfo);
    }

    public void addAccessControlCheckForTable(AccessControlRole accessControlRole, AccessControlInfoForTable accessControlInfoForTable)
    {
        accessControlReferences.addTableReference(accessControlRole, accessControlInfoForTable);
    }

    public void addTableColumnAndSubfieldReferences(
            AccessControl accessControl,
            Identity identity,
            Optional<TransactionId> transactionId,
            AccessControlContext accessControlContext,
            Multimap<QualifiedObjectName, Subfield> tableColumnMap,
            Multimap<QualifiedObjectName, Subfield> tableColumnMapForAccessControl)
    {
        AccessControlInfo accessControlInfo = new AccessControlInfo(accessControl, identity, transactionId, accessControlContext);
        Map<QualifiedObjectName, Set<String>> columnReferences = tableColumnReferences.computeIfAbsent(accessControlInfo, k -> new LinkedHashMap<>());
        tableColumnMap.asMap()
                .forEach((key, value) -> columnReferences.computeIfAbsent(key, k -> new HashSet<>()).addAll(value.stream().map(Subfield::getRootName).collect(toImmutableSet())));

        Map<QualifiedObjectName, Set<Subfield>> columnAndSubfieldReferences = tableColumnAndSubfieldReferences.computeIfAbsent(accessControlInfo, k -> new LinkedHashMap<>());
        tableColumnMapForAccessControl.asMap()
                .forEach((key, value) -> columnAndSubfieldReferences.computeIfAbsent(key, k -> new HashSet<>()).addAll(value));
    }

    public void addEmptyColumnReferencesForTable(AccessControl accessControl, Identity identity, Optional<TransactionId> transactionId, AccessControlContext accessControlContext, QualifiedObjectName table)
    {
        AccessControlInfo accessControlInfo = new AccessControlInfo(accessControl, identity, transactionId, accessControlContext);
        tableColumnReferences.computeIfAbsent(accessControlInfo, k -> new LinkedHashMap<>()).computeIfAbsent(table, k -> new HashSet<>());
        tableColumnAndSubfieldReferences.computeIfAbsent(accessControlInfo, k -> new LinkedHashMap<>()).computeIfAbsent(table, k -> new HashSet<>());
    }

    public Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> getTableColumnReferences()
    {
        return tableColumnReferences;
    }

    public void addUtilizedTableColumnReferences(AccessControlInfo accessControlInfo, Map<QualifiedObjectName, Set<String>> utilizedTableColumns)
    {
        utilizedTableColumnReferences.put(accessControlInfo, utilizedTableColumns);
    }

    public Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> getUtilizedTableColumnReferences()
    {
        return ImmutableMap.copyOf(utilizedTableColumnReferences);
    }

    public void populateTableColumnAndSubfieldReferencesForAccessControl(boolean checkAccessControlOnUtilizedColumnsOnly, boolean checkAccessControlWithSubfields)
    {
        accessControlReferences.addTableColumnAndSubfieldReferencesForAccessControl(getTableColumnAndSubfieldReferencesForAccessControl(checkAccessControlOnUtilizedColumnsOnly, checkAccessControlWithSubfields));
    }

    private Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> getTableColumnAndSubfieldReferencesForAccessControl(boolean checkAccessControlOnUtilizedColumnsOnly, boolean checkAccessControlWithSubfields)
    {
        Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> references;
        if (!checkAccessControlWithSubfields) {
            references = (checkAccessControlOnUtilizedColumnsOnly ? utilizedTableColumnReferences : tableColumnReferences).entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            accessControlEntry -> accessControlEntry.getValue().entrySet().stream().collect(toImmutableMap(
                                    Map.Entry::getKey,
                                    tableEntry -> tableEntry.getValue().stream().map(column -> new Subfield(column, ImmutableList.of())).collect(toImmutableSet())))));
        }
        else if (!checkAccessControlOnUtilizedColumnsOnly) {
            references = tableColumnAndSubfieldReferences;
        }
        else {
            // TODO: Properly support utilized column check. Currently, we prune whole columns, if they are not utilized.
            // We need to generalize it and exclude unutilized subfield references independently.
            references = tableColumnAndSubfieldReferences.entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getKey, accessControlEntry ->
                                    accessControlEntry.getValue().entrySet().stream().collect(toImmutableMap(
                                            Map.Entry::getKey, tableEntry -> tableEntry.getValue().stream().filter(
                                                    column -> {
                                                        Map<QualifiedObjectName, Set<String>> utilizedTableReferences = utilizedTableColumnReferences.get(accessControlEntry.getKey());
                                                        if (utilizedTableReferences == null) {
                                                            return false;
                                                        }
                                                        Set<String> utilizedColumns = utilizedTableReferences.get(tableEntry.getKey());
                                                        return utilizedColumns != null && utilizedColumns.contains(column.getRootName());
                                                    })
                                                    .collect(toImmutableSet())))));
        }
        return buildMaterializedViewAccessControl(references);
    }

    /**
     * For a query on materialized view, only check the actual required access controls for its base tables. For the materialized view,
     * will not check access control by replacing with AllowAllAccessControl.
     **/
    private Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> buildMaterializedViewAccessControl(Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> tableColumnReferences)
    {
        if (!(getStatement() instanceof Query) || materializedViews.isEmpty()) {
            return tableColumnReferences;
        }

        Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> newTableColumnReferences = new LinkedHashMap<>();

        tableColumnReferences.forEach((accessControlInfo, references) -> {
            AccessControlInfo allowAllAccessControlInfo = new AccessControlInfo(new AllowAllAccessControl(), accessControlInfo.getIdentity(), accessControlInfo.getTransactionId(), accessControlInfo.getAccessControlContext());
            Map<QualifiedObjectName, Set<Subfield>> newAllowAllReferences = newTableColumnReferences.getOrDefault(allowAllAccessControlInfo, new LinkedHashMap<>());

            Map<QualifiedObjectName, Set<Subfield>> newOtherReferences = new LinkedHashMap<>();

            references.forEach((table, columns) -> {
                if (materializedViews.containsKey(table)) {
                    newAllowAllReferences.computeIfAbsent(table, key -> new HashSet<>()).addAll(columns);
                }
                else {
                    newOtherReferences.put(table, columns);
                }
            });
            if (!newAllowAllReferences.isEmpty()) {
                newTableColumnReferences.put(allowAllAccessControlInfo, newAllowAllReferences);
            }
            if (!newOtherReferences.isEmpty()) {
                newTableColumnReferences.put(accessControlInfo, newOtherReferences);
            }
        });

        return newTableColumnReferences;
    }

    public void markRedundantOrderBy(OrderBy orderBy)
    {
        redundantOrderBy.add(NodeRef.of(orderBy));
    }

    public boolean isOrderByRedundant(OrderBy orderBy)
    {
        return redundantOrderBy.contains(NodeRef.of(orderBy));
    }

    public void setExpandedQuery(String expandedQuery)
    {
        this.expandedQuery = Optional.of(expandedQuery);
    }

    public Optional<String> getExpandedQuery()
    {
        return expandedQuery;
    }

    public void setCurrentSubquery(QuerySpecification currentSubQuery)
    {
        this.currentQuerySpecification = Optional.of(currentSubQuery);
    }
    public Optional<QuerySpecification> getCurrentQuerySpecification()
    {
        return currentQuerySpecification;
    }

    public Map<FunctionKind, Set<String>> getInvokedFunctions()
    {
        Map<FunctionKind, Set<String>> functionMap = new HashMap<>();
        for (FunctionHandle functionHandle : functionHandles.values()) {
            functionMap.putIfAbsent(functionHandle.getKind(), new HashSet<>());
            functionMap.get(functionHandle.getKind()).add(functionHandle.getName());
        }
        return functionMap.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableSet.copyOf(entry.getValue())));
    }

    public boolean hasRowFilter(QualifiedObjectName table, String identity)
    {
        return rowFilterScopes.contains(new RowFilterScopeEntry(table, identity));
    }

    public void registerTableForRowFiltering(QualifiedObjectName table, String identity)
    {
        rowFilterScopes.add(new RowFilterScopeEntry(table, identity));
    }

    public void unregisterTableForRowFiltering(QualifiedObjectName table, String identity)
    {
        rowFilterScopes.remove(new RowFilterScopeEntry(table, identity));
    }

    public void addRowFilter(Table table, Expression filter)
    {
        rowFilters.computeIfAbsent(NodeRef.of(table), node -> new ArrayList<>())
                .add(filter);
    }

    public List<Expression> getRowFilters(Table node)
    {
        return rowFilters.getOrDefault(NodeRef.of(node), ImmutableList.of());
    }

    public boolean hasColumnMask(QualifiedObjectName table, String column, String identity)
    {
        return columnMaskScopes.contains(new ColumnMaskScopeEntry(table, column, identity));
    }

    public void registerTableForColumnMasking(QualifiedObjectName table, String column, String identity)
    {
        columnMaskScopes.add(new ColumnMaskScopeEntry(table, column, identity));
    }

    public void unregisterTableForColumnMasking(QualifiedObjectName table, String column, String identity)
    {
        columnMaskScopes.remove(new ColumnMaskScopeEntry(table, column, identity));
    }

    public void addColumnMask(Table table, String column, Expression mask)
    {
        Map<String, Expression> masks = columnMasks.computeIfAbsent(NodeRef.of(table), node -> new LinkedHashMap<>());
        checkArgument(!masks.containsKey(column), "Mask already exists for column %s", column);
        masks.put(column, mask);
    }

    public Map<String, Expression> getColumnMasks(Table table)
    {
        return columnMasks.getOrDefault(NodeRef.of(table), ImmutableMap.of());
    }

    public void setTableFunctionAnalysis(TableFunctionInvocation node, TableFunctionInvocationAnalysis analysis)
    {
        tableFunctionAnalyses.put(NodeRef.of(node), analysis);
    }

    public TableFunctionInvocationAnalysis getTableFunctionAnalysis(TableFunctionInvocation node)
    {
        return tableFunctionAnalyses.get(NodeRef.of(node));
    }

    public void setRelationName(Relation relation, QualifiedName name)
    {
        relationNames.put(NodeRef.of(relation), name);
    }

    public QualifiedName getRelationName(Relation relation)
    {
        return relationNames.get(NodeRef.of(relation));
    }

    public void addAliased(Relation relation)
    {
        aliasedRelations.add(NodeRef.of(relation));
    }

    public boolean isAliased(Relation relation)
    {
        return aliasedRelations.contains(NodeRef.of(relation));
    }

    public void addPolymorphicTableFunction(TableFunctionInvocation invocation)
    {
        polymorphicTableFunctions.add(NodeRef.of(invocation));
    }

    public boolean isPolymorphicTableFunction(TableFunctionInvocation invocation)
    {
        return polymorphicTableFunctions.contains(NodeRef.of(invocation));
    }

    @Immutable
    public static final class Insert
    {
        private final TableHandle target;
        private final List<ColumnHandle> columns;

        public Insert(TableHandle target, List<ColumnHandle> columns)
        {
            this.target = requireNonNull(target, "target is null");
            this.columns = requireNonNull(columns, "columns is null");
            checkArgument(columns.size() > 0, "No columns given to insert");
        }

        public List<ColumnHandle> getColumns()
        {
            return columns;
        }

        public TableHandle getTarget()
        {
            return target;
        }
    }

    @Immutable
    public static final class RefreshMaterializedViewAnalysis
    {
        private final TableHandle target;
        private final List<ColumnHandle> columns;
        private final Query query;

        public RefreshMaterializedViewAnalysis(TableHandle target, List<ColumnHandle> columns, Query query)
        {
            this.target = requireNonNull(target, "target is null");
            this.columns = requireNonNull(columns, "columns is null");
            this.query = requireNonNull(query, "query is null");
            checkArgument(columns.size() > 0, "No columns given to insert");
        }

        public List<ColumnHandle> getColumns()
        {
            return columns;
        }

        public TableHandle getTarget()
        {
            return target;
        }

        public Query getQuery()
        {
            return query;
        }
    }

    public static final class JoinUsingAnalysis
    {
        private final List<Integer> leftJoinFields;
        private final List<Integer> rightJoinFields;
        private final List<Integer> otherLeftFields;
        private final List<Integer> otherRightFields;

        JoinUsingAnalysis(List<Integer> leftJoinFields, List<Integer> rightJoinFields, List<Integer> otherLeftFields, List<Integer> otherRightFields)
        {
            this.leftJoinFields = ImmutableList.copyOf(leftJoinFields);
            this.rightJoinFields = ImmutableList.copyOf(rightJoinFields);
            this.otherLeftFields = ImmutableList.copyOf(otherLeftFields);
            this.otherRightFields = ImmutableList.copyOf(otherRightFields);

            checkArgument(leftJoinFields.size() == rightJoinFields.size(), "Expected join fields for left and right to have the same size");
        }

        public List<Integer> getLeftJoinFields()
        {
            return leftJoinFields;
        }

        public List<Integer> getRightJoinFields()
        {
            return rightJoinFields;
        }

        public List<Integer> getOtherLeftFields()
        {
            return otherLeftFields;
        }

        public List<Integer> getOtherRightFields()
        {
            return otherRightFields;
        }
    }

    public static class GroupingSetAnalysis
    {
        private final List<Set<FieldId>> cubes;
        private final List<List<FieldId>> rollups;
        private final List<List<Set<FieldId>>> ordinarySets;
        private final List<Expression> complexExpressions;

        public GroupingSetAnalysis(
                List<Set<FieldId>> cubes,
                List<List<FieldId>> rollups,
                List<List<Set<FieldId>>> ordinarySets,
                List<Expression> complexExpressions)
        {
            this.cubes = ImmutableList.copyOf(cubes);
            this.rollups = ImmutableList.copyOf(rollups);
            this.ordinarySets = ImmutableList.copyOf(ordinarySets);
            this.complexExpressions = ImmutableList.copyOf(complexExpressions);
        }

        public List<Set<FieldId>> getCubes()
        {
            return cubes;
        }

        public List<List<FieldId>> getRollups()
        {
            return rollups;
        }

        public List<List<Set<FieldId>>> getOrdinarySets()
        {
            return ordinarySets;
        }

        public List<Expression> getComplexExpressions()
        {
            return complexExpressions;
        }
    }

    public enum MaterializedViewAnalysisState
    {
        NOT_VISITED(0),
        VISITING(1),
        VISITED(2);

        private final int value;

        MaterializedViewAnalysisState(int value)
        {
            this.value = value;
        }

        public boolean isNotVisited()
        {
            return this.value == NOT_VISITED.value;
        }

        public boolean isVisited()
        {
            return this.value == VISITED.value;
        }

        public boolean isVisiting()
        {
            return this.value == VISITING.value;
        }
    }

    public class NamedQuery
    {
        private final Query query;
        private final boolean isFromView;

        public NamedQuery(Query query, boolean isFromView)
        {
            this.query = query;
            this.isFromView = isFromView;
        }

        public Query getQuery()
        {
            return query;
        }

        public boolean isFromView()
        {
            return isFromView;
        }
    }

    private static class RowFilterScopeEntry
    {
        private final QualifiedObjectName table;
        private final String identity;

        public RowFilterScopeEntry(QualifiedObjectName table, String identity)
        {
            this.table = requireNonNull(table, "table is null");
            this.identity = requireNonNull(identity, "identity is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RowFilterScopeEntry that = (RowFilterScopeEntry) o;
            return table.equals(that.table) &&
                    identity.equals(that.identity);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, identity);
        }
    }

    private static class ColumnMaskScopeEntry
    {
        private final QualifiedObjectName table;
        private final String column;
        private final String identity;

        public ColumnMaskScopeEntry(QualifiedObjectName table, String column, String identity)
        {
            this.table = requireNonNull(table, "table is null");
            this.column = requireNonNull(column, "column is null");
            this.identity = requireNonNull(identity, "identity is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnMaskScopeEntry that = (ColumnMaskScopeEntry) o;
            return table.equals(that.table) &&
                    column.equals(that.column) &&
                    identity.equals(that.identity);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, column, identity);
        }
    }

    public static class TableArgumentAnalysis
    {
        private final String argumentName;
        private final Optional<QualifiedName> name;
        private final Relation relation;
        private final Optional<List<Expression>> partitionBy; // it is allowed to partition by empty list
        private final Optional<OrderBy> orderBy;
        private final boolean pruneWhenEmpty;
        private final boolean rowSemantics;
        private final boolean passThroughColumns;

        private TableArgumentAnalysis(
                String argumentName,
                Optional<QualifiedName> name,
                Relation relation,
                Optional<List<Expression>> partitionBy,
                Optional<OrderBy> orderBy,
                boolean pruneWhenEmpty,
                boolean rowSemantics,
                boolean passThroughColumns)
        {
            this.argumentName = requireNonNull(argumentName, "argumentName is null");
            this.name = requireNonNull(name, "name is null");
            this.relation = requireNonNull(relation, "relation is null");
            this.partitionBy = requireNonNull(partitionBy, "partitionBy is null").map(ImmutableList::copyOf);
            this.orderBy = requireNonNull(orderBy, "orderBy is null");
            this.pruneWhenEmpty = pruneWhenEmpty;
            this.rowSemantics = rowSemantics;
            this.passThroughColumns = passThroughColumns;
        }

        public String getArgumentName()
        {
            return argumentName;
        }

        public Optional<QualifiedName> getName()
        {
            return name;
        }

        public Relation getRelation()
        {
            return relation;
        }

        public Optional<List<Expression>> getPartitionBy()
        {
            return partitionBy;
        }

        public Optional<OrderBy> getOrderBy()
        {
            return orderBy;
        }

        public boolean isPruneWhenEmpty()
        {
            return pruneWhenEmpty;
        }

        public boolean isRowSemantics()
        {
            return rowSemantics;
        }

        public boolean isPassThroughColumns()
        {
            return passThroughColumns;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static final class Builder
        {
            private String argumentName;
            private Optional<QualifiedName> name = Optional.empty();
            private Relation relation;
            private Optional<List<Expression>> partitionBy = Optional.empty();
            private Optional<OrderBy> orderBy = Optional.empty();
            private boolean pruneWhenEmpty;
            private boolean rowSemantics;
            private boolean passThroughColumns;

            private Builder() {}

            public Builder withArgumentName(String argumentName)
            {
                this.argumentName = argumentName;
                return this;
            }

            public Builder withName(QualifiedName name)
            {
                this.name = Optional.of(name);
                return this;
            }

            public Builder withRelation(Relation relation)
            {
                this.relation = relation;
                return this;
            }

            public Builder withPartitionBy(List<Expression> partitionBy)
            {
                this.partitionBy = Optional.of(partitionBy);
                return this;
            }

            public Builder withOrderBy(OrderBy orderBy)
            {
                this.orderBy = Optional.of(orderBy);
                return this;
            }

            public Builder withPruneWhenEmpty(boolean pruneWhenEmpty)
            {
                this.pruneWhenEmpty = pruneWhenEmpty;
                return this;
            }

            public Builder withRowSemantics(boolean rowSemantics)
            {
                this.rowSemantics = rowSemantics;
                return this;
            }

            public Builder withPassThroughColumns(boolean passThroughColumns)
            {
                this.passThroughColumns = passThroughColumns;
                return this;
            }

            public TableArgumentAnalysis build()
            {
                return new TableArgumentAnalysis(argumentName, name, relation, partitionBy, orderBy, pruneWhenEmpty, rowSemantics, passThroughColumns);
            }
        }
    }

    public static class TableFunctionInvocationAnalysis
    {
        private final ConnectorId connectorId;
        private final String functionName;
        private final Map<String, Argument> arguments;
        private final List<TableArgumentAnalysis> tableArgumentAnalyses;
        private final List<List<String>> copartitioningLists;
        private final Map<String, List<Integer>> requiredColumns;
        private final int properColumnsCount;
        private final ConnectorTableFunctionHandle connectorTableFunctionHandle;
        private final ConnectorTransactionHandle transactionHandle;

        public TableFunctionInvocationAnalysis(
                ConnectorId connectorId,
                String functionName,
                Map<String, Argument> arguments,
                List<TableArgumentAnalysis> tableArgumentAnalyses,
                Map<String, List<Integer>> requiredColumns,
                List<List<String>> copartitioningLists,
                int properColumnsCount,
                ConnectorTableFunctionHandle connectorTableFunctionHandle,
                ConnectorTransactionHandle transactionHandle)
        {
            this.connectorId = requireNonNull(connectorId, "connectorId is null");
            this.functionName = requireNonNull(functionName, "functionName is null");
            this.arguments = ImmutableMap.copyOf(arguments);
            this.connectorTableFunctionHandle = requireNonNull(connectorTableFunctionHandle, "connectorTableFunctionHandle is null");
            this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
            this.tableArgumentAnalyses = ImmutableList.copyOf(tableArgumentAnalyses);
            this.requiredColumns = requiredColumns.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue())));
            this.copartitioningLists = ImmutableList.copyOf(copartitioningLists);
            this.properColumnsCount = properColumnsCount;
        }

        public ConnectorId getConnectorId()
        {
            return connectorId;
        }

        public String getFunctionName()
        {
            return functionName;
        }

        public Map<String, Argument> getArguments()
        {
            return arguments;
        }

        public List<TableArgumentAnalysis> getTableArgumentAnalyses()
        {
            return tableArgumentAnalyses;
        }

        public Map<String, List<Integer>> getRequiredColumns()
        {
            return requiredColumns;
        }

        public List<List<String>> getCopartitioningLists()
        {
            return copartitioningLists;
        }

        /**
         * Proper columns are the columns produced by the table function, as opposed to pass-through columns from input tables.
         * Proper columns should be considered the actual result of the table function.
         * @return the number of table function's proper columns
         */
        public int getProperColumnsCount()
        {
            return properColumnsCount;
        }

        public ConnectorTableFunctionHandle getConnectorTableFunctionHandle()
        {
            return connectorTableFunctionHandle;
        }

        public ConnectorTransactionHandle getTransactionHandle()
        {
            return transactionHandle;
        }
    }
}
