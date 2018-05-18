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

import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
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
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.util.MoreLists.listOfListsCopy;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
    private final List<Expression> parameters;
    private String updateType;

    private final Map<NodeRef<Table>, Query> namedQueries = new LinkedHashMap<>();

    private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, FieldId> columnReferences = new LinkedHashMap<>();
    private final Map<QualifiedObjectName, Set<String>> tableColumnReferences = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, List<FunctionCall>> aggregates = new LinkedHashMap<>();
    private final Map<NodeRef<OrderBy>, List<Expression>> orderByAggregates = new LinkedHashMap<>();
    private final Map<NodeRef<QuerySpecification>, List<List<Expression>>> groupByExpressions = new LinkedHashMap<>();
    private final Map<NodeRef<Node>, Expression> where = new LinkedHashMap<>();
    private final Map<NodeRef<QuerySpecification>, Expression> having = new LinkedHashMap<>();
    private final Map<NodeRef<Node>, List<Expression>> orderByExpressions = new LinkedHashMap<>();
    private final Map<NodeRef<Node>, List<Expression>> outputExpressions = new LinkedHashMap<>();
    private final Map<NodeRef<QuerySpecification>, List<FunctionCall>> windowFunctions = new LinkedHashMap<>();
    private final Map<NodeRef<OrderBy>, List<FunctionCall>> orderByWindowFunctions = new LinkedHashMap<>();

    private final Map<NodeRef<Join>, Expression> joins = new LinkedHashMap<>();
    private final Map<NodeRef<Join>, JoinUsingAnalysis> joinUsing = new LinkedHashMap<>();

    private final ListMultimap<NodeRef<Node>, InPredicate> inPredicatesSubqueries = ArrayListMultimap.create();
    private final ListMultimap<NodeRef<Node>, SubqueryExpression> scalarSubqueries = ArrayListMultimap.create();
    private final ListMultimap<NodeRef<Node>, ExistsPredicate> existsSubqueries = ArrayListMultimap.create();
    private final ListMultimap<NodeRef<Node>, QuantifiedComparisonExpression> quantifiedComparisonSubqueries = ArrayListMultimap.create();

    private final Map<NodeRef<Table>, TableHandle> tables = new LinkedHashMap<>();

    private final Map<NodeRef<Expression>, Type> types = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, Type> coercions = new LinkedHashMap<>();
    private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();
    private final Map<NodeRef<Relation>, List<Type>> relationCoercions = new LinkedHashMap<>();
    private final Map<NodeRef<FunctionCall>, Signature> functionSignature = new LinkedHashMap<>();
    private final Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences = new LinkedHashMap<>();

    private final Map<Field, ColumnHandle> columns = new LinkedHashMap<>();

    private final Map<NodeRef<SampledRelation>, Double> sampleRatios = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, List<GroupingOperation>> groupingOperations = new LinkedHashMap<>();

    // for create table
    private Optional<QualifiedObjectName> createTableDestination = Optional.empty();
    private Map<String, Expression> createTableProperties = ImmutableMap.of();
    private boolean createTableAsSelectWithData = true;
    private boolean createTableAsSelectNoOp;
    private Optional<List<Identifier>> createTableColumnAliases = Optional.empty();
    private Optional<String> createTableComment = Optional.empty();

    private Optional<Insert> insert = Optional.empty();

    // for describe input and describe output
    private final boolean isDescribe;

    // for recursive view detection
    private final Deque<Table> tablesForView = new ArrayDeque<>();

    public Analysis(@Nullable Statement root, List<Expression> parameters, boolean isDescribe)
    {
        requireNonNull(parameters);

        this.root = root;
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        this.isDescribe = isDescribe;
    }

    public Statement getStatement()
    {
        return root;
    }

    public String getUpdateType()
    {
        return updateType;
    }

    public void setUpdateType(String updateType)
    {
        this.updateType = updateType;
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

    public void setGroupingSets(QuerySpecification node, List<List<Expression>> expressions)
    {
        groupByExpressions.put(NodeRef.of(node), listOfListsCopy(expressions));
    }

    public boolean isTypeOnlyCoercion(Expression expression)
    {
        return typeOnlyCoercions.contains(NodeRef.of(expression));
    }

    public List<List<Expression>> getGroupingSets(QuerySpecification node)
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
        this.columnReferences.putAll(columnReferences);
    }

    public Scope getScope(Node node)
    {
        return tryGetScope(node).orElseThrow(() -> new IllegalArgumentException(String.format("Analysis does not contain information for node: %s", node)));
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

    public TableHandle getTableHandle(Table table)
    {
        return tables.get(NodeRef.of(table));
    }

    public Collection<TableHandle> getTables()
    {
        return unmodifiableCollection(tables.values());
    }

    public void registerTable(Table table, TableHandle handle)
    {
        tables.put(NodeRef.of(table), handle);
    }

    public Signature getFunctionSignature(FunctionCall function)
    {
        return functionSignature.get(NodeRef.of(function));
    }

    public void addFunctionSignatures(Map<NodeRef<FunctionCall>, Signature> infos)
    {
        functionSignature.putAll(infos);
    }

    public Set<NodeRef<Expression>> getColumnReferences()
    {
        return unmodifiableSet(columnReferences.keySet());
    }

    public Map<NodeRef<Expression>, FieldId> getColumnReferenceFields()
    {
        return unmodifiableMap(columnReferences);
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

    public void addCoercions(Map<NodeRef<Expression>, Type> coercions, Set<NodeRef<Expression>> typeOnlyCoercions)
    {
        this.coercions.putAll(coercions);
        this.typeOnlyCoercions.addAll(typeOnlyCoercions);
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

    public Query getNamedQuery(Table table)
    {
        return namedQueries.get(NodeRef.of(table));
    }

    public void registerNamedQuery(Table tableReference, Query query)
    {
        requireNonNull(tableReference, "tableReference is null");
        requireNonNull(query, "query is null");

        namedQueries.put(NodeRef.of(tableReference), query);
    }

    public void registerTableForView(Table tableReference)
    {
        tablesForView.push(requireNonNull(tableReference, "table is null"));
    }

    public void unregisterTableForView()
    {
        tablesForView.pop();
    }

    public boolean hasTableInView(Table tableReference)
    {
        return tablesForView.contains(tableReference);
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

    public List<Expression> getParameters()
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

    public void addTableColumnReferences(Multimap<QualifiedObjectName, String> tableColumnMap)
    {
        tableColumnMap.asMap()
                .forEach((key, value) -> tableColumnReferences.computeIfAbsent(key, k -> new HashSet<>()).addAll(value));
    }

    public void addEmptyColumnReferencesForTable(QualifiedObjectName table)
    {
        this.tableColumnReferences.putIfAbsent(table, new HashSet<>());
    }

    public Map<QualifiedObjectName, Set<String>> getTableColumnReferences()
    {
        return tableColumnReferences;
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
}
