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
import com.facebook.presto.util.maps.IdentityLinkedHashMap;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class Analysis
{
    private final Statement root;
    private final List<Expression> parameters;
    private String updateType;

    private final Map<NodeRef<Table>, Query> namedQueries = new LinkedHashMap<>();

    private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, FieldId> columnReferences = new LinkedHashMap<>();

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
    // TODO ListMultimap too
    private final ListMultimap<Node, InPredicate> inPredicatesSubqueries = ArrayListMultimap.create();
    private final ListMultimap<Node, SubqueryExpression> scalarSubqueries = ArrayListMultimap.create();
    private final ListMultimap<Node, ExistsPredicate> existsSubqueries = ArrayListMultimap.create();
    private final ListMultimap<Node, QuantifiedComparisonExpression> quantifiedComparisonSubqueries = ArrayListMultimap.create();

    private final Map<NodeRef<Table>, TableHandle> tables = new LinkedHashMap<>();

    private final Map<NodeRef<Expression>, Type> types = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, Type> coercions = new LinkedHashMap<>();
    private final Set<NodeRef<Expression>> typeOnlyCoercions = newSetFromMap(new IdentityLinkedHashMap<>());
    private final Map<NodeRef<Relation>, Type[]> relationCoercions = new LinkedHashMap<>();
    private final Map<NodeRef<FunctionCall>, Signature> functionSignature = new LinkedHashMap<>();
    private final Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences = new LinkedHashMap<>();

    private final Map<Field, ColumnHandle> columns = new LinkedHashMap<>();

    private final Map<NodeRef<SampledRelation>, Double> sampleRatios = new LinkedHashMap<>();

    // for create table
    private Optional<QualifiedObjectName> createTableDestination = Optional.empty();
    private Map<String, Expression> createTableProperties = ImmutableMap.of();
    private boolean createTableAsSelectWithData = true;
    private boolean createTableAsSelectNoOp = false;
    private Optional<String> createTableComment = Optional.empty();

    private Optional<Insert> insert = Optional.empty();

    // for describe input and describe output
    private final boolean isDescribe;

    // for recursive view detection
    private final Deque<Table> tablesForView = new ArrayDeque<>();

    public Analysis(Statement root, List<Expression> parameters, boolean isDescribe)
    {
        requireNonNull(parameters);

        this.root = root;
        this.parameters = parameters;
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
        this.aggregates.put(NodeRef.of(node), aggregates);
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

    public IdentityLinkedHashMap<Expression, Type> getTypes()
    {
        return NodeRefCollections.toIdentityMap(types);
    }

    public Type getType(Expression expression)
    {
        NodeRef<Expression> key = NodeRef.of(expression);
        checkArgument(types.containsKey(key), "Expression not analyzed: %s", expression);
        return types.get(key);
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
        return relationCoercions.get(NodeRef.of(relation));
    }

    public void addRelationCoercion(Relation relation, Type[] types)
    {
        relationCoercions.put(NodeRef.of(relation), types);
    }

    public IdentityLinkedHashMap<Expression, Type> getCoercions()
    {
        return NodeRefCollections.toIdentityMap(coercions);
    }

    public Type getCoercion(Expression expression)
    {
        return coercions.get(NodeRef.of(expression));
    }

    public void addLambdaArgumentReferences(IdentityLinkedHashMap<Identifier, LambdaArgumentDeclaration> lambdaArgumentReferences)
    {
        this.lambdaArgumentReferences.putAll(NodeRefCollections.fromIdentityMap(lambdaArgumentReferences));
    }

    public LambdaArgumentDeclaration getLambdaArgumentReference(Identifier identifier)
    {
        return lambdaArgumentReferences.get(NodeRef.of(identifier));
    }

    public IdentityLinkedHashMap<Identifier, LambdaArgumentDeclaration> getLambdaArgumentReferences()
    {
        return NodeRefCollections.toIdentityMap(lambdaArgumentReferences);
    }

    public void setGroupingSets(QuerySpecification node, List<List<Expression>> expressions)
    {
        groupByExpressions.put(NodeRef.of(node), expressions);
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
        orderByExpressions.put(NodeRef.of(node), items);
    }

    public List<Expression> getOrderByExpressions(Node node)
    {
        return orderByExpressions.get(NodeRef.of(node));
    }

    public void setOutputExpressions(Node node, List<Expression> expressions)
    {
        outputExpressions.put(NodeRef.of(node), expressions);
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
        this.inPredicatesSubqueries.putAll(node, expressionAnalysis.getSubqueryInPredicates());
        this.scalarSubqueries.putAll(node, expressionAnalysis.getScalarSubqueries());
        this.existsSubqueries.putAll(node, expressionAnalysis.getExistsSubqueries());
        this.quantifiedComparisonSubqueries.putAll(node, expressionAnalysis.getQuantifiedComparisons());
    }

    public List<InPredicate> getInPredicateSubqueries(Node node)
    {
        if (inPredicatesSubqueries.containsKey(node)) {
            return inPredicatesSubqueries.get(node);
        }
        return ImmutableList.of();
    }

    public List<SubqueryExpression> getScalarSubqueries(Node node)
    {
        if (scalarSubqueries.containsKey(node)) {
            return scalarSubqueries.get(node);
        }
        return ImmutableList.of();
    }

    public List<ExistsPredicate> getExistsSubqueries(Node node)
    {
        if (existsSubqueries.containsKey(node)) {
            return existsSubqueries.get(node);
        }
        return ImmutableList.of();
    }

    public List<QuantifiedComparisonExpression> getQuantifiedComparisonSubqueries(Node node)
    {
        if (quantifiedComparisonSubqueries.containsKey(node)) {
            return quantifiedComparisonSubqueries.get(node);
        }
        return ImmutableList.of();
    }

    public void setWindowFunctions(QuerySpecification node, List<FunctionCall> functions)
    {
        windowFunctions.put(NodeRef.of(node), functions);
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

    public void addColumnReferences(IdentityLinkedHashMap<Expression, FieldId> columnReferences)
    {
        this.columnReferences.putAll(NodeRefCollections.fromIdentityMap(columnReferences));
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
        return tables.values();
    }

    public void registerTable(Table table, TableHandle handle)
    {
        tables.put(NodeRef.of(table), handle);
    }

    public Signature getFunctionSignature(FunctionCall function)
    {
        return functionSignature.get(NodeRef.of(function));
    }

    public void addFunctionSignatures(IdentityLinkedHashMap<FunctionCall, Signature> infos)
    {
        functionSignature.putAll(NodeRefCollections.fromIdentityMap(infos));
    }

    public Set<Expression> getColumnReferences()
    {
        return unmodifiableSet(NodeRefCollections.toIdentityMap(columnReferences).keySet());
    }

    public Map<Expression, FieldId> getColumnReferenceFields()
    {
        return unmodifiableMap(NodeRefCollections.toIdentityMap(columnReferences));
    }

    public void addTypes(IdentityLinkedHashMap<Expression, Type> types)
    {
        this.types.putAll(NodeRefCollections.fromIdentityMap(types));
    }

    public void addCoercion(Expression expression, Type type, boolean isTypeOnlyCoercion)
    {
        this.coercions.put(NodeRef.of(expression), type);
        if (isTypeOnlyCoercion) {
            this.typeOnlyCoercions.add(NodeRef.of(expression));
        }
    }

    public void addCoercions(IdentityLinkedHashMap<Expression, Type> coercions, Set<Expression> typeOnlyCoercions)
    {
        this.coercions.putAll(NodeRefCollections.fromIdentityMap(coercions));
        this.typeOnlyCoercions.addAll(NodeRefCollections.fromIdentitySet(typeOnlyCoercions));
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
        this.createTableProperties = createTableProperties;
    }

    public Map<String, Expression> getCreateTableProperties()
    {
        return createTableProperties;
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

    public List<Expression> getParameters()
    {
        return parameters;
    }

    public boolean isDescribe()
    {
        return isDescribe;
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
}
