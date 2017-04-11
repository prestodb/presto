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
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class Analysis
{
    private final Statement root;
    private final List<Expression> parameters;
    private String updateType;

    private final IdentityLinkedHashMap<Table, Query> namedQueries = new IdentityLinkedHashMap<>();

    private final IdentityLinkedHashMap<Node, Scope> scopes = new IdentityLinkedHashMap<>();
    private final Set<Expression> columnReferences = newSetFromMap(new IdentityLinkedHashMap<>());

    private final IdentityLinkedHashMap<QuerySpecification, List<FunctionCall>> aggregates = new IdentityLinkedHashMap<>();
    private final IdentityLinkedHashMap<OrderBy, List<Expression>> orderByAggregates = new IdentityLinkedHashMap<>();
    private final IdentityLinkedHashMap<QuerySpecification, List<List<Expression>>> groupByExpressions = new IdentityLinkedHashMap<>();
    private final IdentityLinkedHashMap<Node, Expression> where = new IdentityLinkedHashMap<>();
    private final IdentityLinkedHashMap<QuerySpecification, Expression> having = new IdentityLinkedHashMap<>();
    private final IdentityLinkedHashMap<Node, List<Expression>> orderByExpressions = new IdentityLinkedHashMap<>();
    private final IdentityLinkedHashMap<Node, List<Expression>> outputExpressions = new IdentityLinkedHashMap<>();
    private final IdentityLinkedHashMap<QuerySpecification, List<FunctionCall>> windowFunctions = new IdentityLinkedHashMap<>();
    private final IdentityLinkedHashMap<OrderBy, List<FunctionCall>> orderByWindowFunctions = new IdentityLinkedHashMap<>();

    private final IdentityLinkedHashMap<Join, Expression> joins = new IdentityLinkedHashMap<>();
    private final ListMultimap<Node, InPredicate> inPredicatesSubqueries = ArrayListMultimap.create();
    private final ListMultimap<Node, SubqueryExpression> scalarSubqueries = ArrayListMultimap.create();
    private final ListMultimap<Node, ExistsPredicate> existsSubqueries = ArrayListMultimap.create();
    private final ListMultimap<Node, QuantifiedComparisonExpression> quantifiedComparisonSubqueries = ArrayListMultimap.create();

    private final IdentityLinkedHashMap<Table, TableHandle> tables = new IdentityLinkedHashMap<>();

    private final IdentityLinkedHashMap<Expression, Type> types = new IdentityLinkedHashMap<>();
    private final IdentityLinkedHashMap<Expression, Type> coercions = new IdentityLinkedHashMap<>();
    private final Set<Expression> typeOnlyCoercions = newSetFromMap(new IdentityLinkedHashMap<>());
    private final IdentityLinkedHashMap<Relation, Type[]> relationCoercions = new IdentityLinkedHashMap<>();
    private final IdentityLinkedHashMap<FunctionCall, Signature> functionSignature = new IdentityLinkedHashMap<>();
    private final IdentityLinkedHashMap<Identifier, LambdaArgumentDeclaration> lambdaArgumentReferences = new IdentityLinkedHashMap<>();

    private final IdentityLinkedHashMap<Field, ColumnHandle> columns = new IdentityLinkedHashMap<>();

    private final IdentityLinkedHashMap<SampledRelation, Double> sampleRatios = new IdentityLinkedHashMap<>();

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
        this.aggregates.put(node, aggregates);
    }

    public List<FunctionCall> getAggregates(QuerySpecification query)
    {
        return aggregates.get(query);
    }

    public void setOrderByAggregates(OrderBy node, List<Expression> aggregates)
    {
        this.orderByAggregates.put(node, ImmutableList.copyOf(aggregates));
    }

    public List<Expression> getOrderByAggregates(OrderBy query)
    {
        return orderByAggregates.get(query);
    }

    public IdentityLinkedHashMap<Expression, Type> getTypes()
    {
        return new IdentityLinkedHashMap<>(types);
    }

    public Type getType(Expression expression)
    {
        checkArgument(types.containsKey(expression), "Expression not analyzed: %s", expression);
        return types.get(expression);
    }

    public Type getTypeWithCoercions(Expression expression)
    {
        checkArgument(types.containsKey(expression), "Expression not analyzed: %s", expression);
        if (coercions.containsKey(expression)) {
            return coercions.get(expression);
        }
        return types.get(expression);
    }

    public Type[] getRelationCoercion(Relation relation)
    {
        return relationCoercions.get(relation);
    }

    public void addRelationCoercion(Relation relation, Type[] types)
    {
        relationCoercions.put(relation, types);
    }

    public IdentityLinkedHashMap<Expression, Type> getCoercions()
    {
        return coercions;
    }

    public Type getCoercion(Expression expression)
    {
        return coercions.get(expression);
    }

    public void addLambdaArgumentReferences(IdentityLinkedHashMap<Identifier, LambdaArgumentDeclaration> lambdaArgumentReferences)
    {
        this.lambdaArgumentReferences.putAll(lambdaArgumentReferences);
    }

    public LambdaArgumentDeclaration getLambdaArgumentReference(Identifier identifier)
    {
        return lambdaArgumentReferences.get(identifier);
    }

    public IdentityLinkedHashMap<Identifier, LambdaArgumentDeclaration> getLambdaArgumentReferences()
    {
        return lambdaArgumentReferences;
    }

    public void setGroupingSets(QuerySpecification node, List<List<Expression>> expressions)
    {
        groupByExpressions.put(node, expressions);
    }

    public boolean isTypeOnlyCoercion(Expression expression)
    {
        return typeOnlyCoercions.contains(expression);
    }

    public List<List<Expression>> getGroupingSets(QuerySpecification node)
    {
        return groupByExpressions.get(node);
    }

    public void setWhere(Node node, Expression expression)
    {
        where.put(node, expression);
    }

    public Expression getWhere(QuerySpecification node)
    {
        return where.get(node);
    }

    public void setOrderByExpressions(Node node, List<Expression> items)
    {
        orderByExpressions.put(node, items);
    }

    public List<Expression> getOrderByExpressions(Node node)
    {
        return orderByExpressions.get(node);
    }

    public void setOutputExpressions(Node node, List<Expression> expressions)
    {
        outputExpressions.put(node, expressions);
    }

    public List<Expression> getOutputExpressions(Node node)
    {
        return outputExpressions.get(node);
    }

    public void setHaving(QuerySpecification node, Expression expression)
    {
        having.put(node, expression);
    }

    public void setJoinCriteria(Join node, Expression criteria)
    {
        joins.put(node, criteria);
    }

    public Expression getJoinCriteria(Join join)
    {
        return joins.get(join);
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
        windowFunctions.put(node, functions);
    }

    public List<FunctionCall> getWindowFunctions(QuerySpecification query)
    {
        return windowFunctions.get(query);
    }

    public void setOrderByWindowFunctions(OrderBy node, List<FunctionCall> functions)
    {
        orderByWindowFunctions.put(node, ImmutableList.copyOf(functions));
    }

    public List<FunctionCall> getOrderByWindowFunctions(OrderBy query)
    {
        return orderByWindowFunctions.get(query);
    }

    public void addColumnReferences(Set<Expression> columnReferences)
    {
        this.columnReferences.addAll(columnReferences);
    }

    public Scope getScope(Node node)
    {
        return tryGetScope(node).orElseThrow(() -> new IllegalArgumentException(String.format("Analysis does not contain information for node: %s", node)));
    }

    public Optional<Scope> tryGetScope(Node node)
    {
        if (scopes.containsKey(node)) {
            return Optional.of(scopes.get(node));
        }

        return Optional.empty();
    }

    public Scope getRootScope()
    {
        return getScope(root);
    }

    public void setScope(Node node, Scope scope)
    {
        scopes.put(node, scope);
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
        return tables.get(table);
    }

    public Collection<TableHandle> getTables()
    {
        return tables.values();
    }

    public void registerTable(Table table, TableHandle handle)
    {
        tables.put(table, handle);
    }

    public Signature getFunctionSignature(FunctionCall function)
    {
        return functionSignature.get(function);
    }

    public void addFunctionSignatures(IdentityLinkedHashMap<FunctionCall, Signature> infos)
    {
        functionSignature.putAll(infos);
    }

    public Set<Expression> getColumnReferences()
    {
        return unmodifiableSet(columnReferences);
    }

    public void addTypes(IdentityLinkedHashMap<Expression, Type> types)
    {
        this.types.putAll(types);
    }

    public void addCoercion(Expression expression, Type type, boolean isTypeOnlyCoercion)
    {
        this.coercions.put(expression, type);
        if (isTypeOnlyCoercion) {
            this.typeOnlyCoercions.add(expression);
        }
    }

    public void addCoercions(IdentityLinkedHashMap<Expression, Type> coercions, Set<Expression> typeOnlyCoercions)
    {
        this.coercions.putAll(coercions);
        this.typeOnlyCoercions.addAll(typeOnlyCoercions);
    }

    public Expression getHaving(QuerySpecification query)
    {
        return having.get(query);
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
        return namedQueries.get(table);
    }

    public void registerNamedQuery(Table tableReference, Query query)
    {
        requireNonNull(tableReference, "tableReference is null");
        requireNonNull(query, "query is null");

        namedQueries.put(tableReference, query);
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
        sampleRatios.put(relation, ratio);
    }

    public double getSampleRatio(SampledRelation relation)
    {
        Preconditions.checkState(sampleRatios.containsKey(relation), "Sample ratio missing for %s. Broken analysis?", relation);
        return sampleRatios.get(relation);
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
