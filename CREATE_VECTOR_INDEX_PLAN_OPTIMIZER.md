# CREATE VECTOR INDEX - Plan Optimizer Rewrite Implementation Plan

## Overview

This document describes how to implement `CREATE VECTOR INDEX` syntax that gets transformed **at the Plan Optimizer level** into a simple SELECT statement that calls a Python UDF with metadata only.

**Key Design Decision:** The UDF receives **metadata only** (table name, columns, WHERE predicate as string, properties). The UDF then triggers a **remote service** that handles all data access and index building.

### Transformation

**Input SQL:**
```sql
CREATE VECTOR INDEX vector_index ON candidates(id, embedding)
WHERE ds = '2026-01-01'
WITH (
    index_type = 'ivf_rabitq4',
    distance_metric = 'cosine',
    index_options = 'nlist=100000,nb=8'
)
```

**Initial Plan (from LogicalPlanner):**
```
CreateVectorIndexNode
  indexName: "vector_index"
  tableName: "candidates"
  columns: [id, embedding]
  wherePredicate: Expression (ds = '2026-01-01')  ← stored as AST, formatted to string later
  properties: {index_type: 'ivf_rabitq4', ...}
```

**Rewritten Plan (after optimizer rule):**
```
ProjectNode [create_local_index('candidates', ARRAY['id','embedding'], 'ds = ''2026-01-01''', MAP(...))]
  └─ ValuesNode (single empty row)
```

**Equivalent SQL after rewrite:**
```sql
SELECT create_local_index(
    'candidates',
    ARRAY['id', 'embedding'],
    'ds = ''2026-01-01''',
    MAP(
        ARRAY['index_type', 'distance_metric', 'index_options'],
        ARRAY['ivf_rabitq4', 'cosine', 'nlist=100000,nb=8']
    )
) AS result
```

---

## Key Design Decisions

### 1. UDF Receives Metadata Only

The `create_local_index` UDF does **NOT** receive actual row data. It receives:
- `table_name` (VARCHAR): Fully qualified table name
- `columns` (ARRAY(VARCHAR)): Column names as strings
- `where_predicate` (VARCHAR): WHERE clause as SQL string (can be NULL)
- `properties` (MAP(VARCHAR, VARCHAR)): Configuration options

The UDF triggers a **remote service** that:
1. Receives the metadata
2. Executes its own query to fetch data from the table
3. Builds the vector index
4. Returns success/failure

### 2. No TableScanNode or FilterNode Needed

Since we don't pass data through the query plan:
- ✅ Use `ValuesNode` (single empty row) to trigger one UDF call
- ❌ No `TableScanNode` needed
- ❌ No `FilterNode` needed
- ❌ No column handles or variable references for data columns

### 3. WHERE Clause as String

The WHERE clause is converted to a SQL string using `SqlFormatter`:
```java
String whereString = node.getWhere()
    .map(expr -> SqlFormatter.formatSql(expr, Optional.empty()))
    .orElse(null);
```

### 4. QueryType.SELECT for Execution Path

Register `CreateVectorIndex` as `QueryType.SELECT` in `StatementUtils.java` to:
- Route through `SqlQueryExecution` (not DDL path)
- Enable LogicalPlanner and Optimizer
- Allow the optimizer rule to transform the plan

---

## Architecture: Where Transformation Happens

```
SQL String: "CREATE VECTOR INDEX ..."
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 1: PARSING                                                   │
│  SqlParser.createStatement()                                        │
│  → Creates CreateVectorIndex AST node                               │
└─────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
              CreateVectorIndex AST
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 2: QUERY TYPE ROUTING                                        │
│  StatementUtils.getQueryType(CreateVectorIndex.class)               │
│  → Returns QueryType.SELECT (NOT DATA_DEFINITION!)                  │
│  → Routes to SqlQueryExecution (enables LogicalPlanner + Optimizer) │
└─────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 3: SEMANTIC ANALYSIS                                         │
│  StatementAnalyzer.visitCreateVectorIndex()                         │
│  → Validates table exists                                           │
│  → Validates columns exist                                          │
│  → Validates WHERE clause (if present)                              │
│  → Validates properties                                             │
└─────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
                 Analysis
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 4: LOGICAL PLANNING                                          │
│  LogicalPlanner.createVectorIndexPlan()                             │
│  → Creates CreateVectorIndexNode (leaf node, no sources)            │
│  → Stores metadata for optimizer rule                               │
└─────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
         CreateVectorIndexNode (Plan Node)
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 5: PLAN OPTIMIZATION  ← ★ THIS IS WHERE WE TRANSFORM ★      │
│  IterativeOptimizer with RewriteCreateVectorIndex rule             │
│  → Transforms CreateVectorIndexNode to:                             │
│     ProjectNode(ValuesNode)                                         │
│  → ProjectNode contains create_local_index() UDF call               │
└─────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
         ProjectNode → ValuesNode
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 6: QUERY EXECUTION                                           │
│  SqlQueryExecution                                                  │
│  → Executes the rewritten plan                                      │
│  → Calls create_local_index UDF ONCE                                │
│  → UDF triggers remote service                                      │
│  → Returns BOOLEAN result                                           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Files to Modify/Create

| # | File | Action | Description |
|---|------|--------|-------------|
| **Parser (presto-parser)** |||
| 1 | `SqlBase.g4` | Modify | Add grammar rule |
| 2 | `CreateVectorIndex.java` | **Create** | AST node class |
| 3 | `AstBuilder.java` | Modify | Parse context to AST |
| 4 | `AstVisitor.java` | Modify | Add visitor method |
| 5 | `DefaultTraversalVisitor.java` | Modify | Add traversal |
| 6 | `SqlFormatter.java` | Modify | Format AST to SQL |
| **Analyzer (presto-analyzer)** |||
| 7 | `StatementUtils.java` | Modify | Register as QueryType.SELECT |
| **Analyzer (presto-main-base)** |||
| 8 | `StatementAnalyzer.java` | Modify | Semantic validation |
| 9 | `Analysis.java` | Modify | Add storage for CreateVectorIndex metadata |
| **Planner (presto-main-base)** |||
| 10 | `CreateVectorIndexNode.java` | **Create** | Plan node (simplified) |
| 11 | `InternalPlanVisitor.java` | Modify | Add visitor method |
| 12 | `Patterns.java` | Modify | Add pattern factory |
| 13 | `LogicalPlanner.java` | Modify | Create initial plan |
| **Optimizer (presto-main-base)** |||
| 14 | `RewriteCreateVectorIndex.java` | **Create** | Optimizer rule |
| 15 | `PlanOptimizers.java` | Modify | Register rule |
| **Tests** |||
| 16 | `TestSqlParser.java` | Modify | Parser tests |
| 17 | `TestRewriteCreateVectorIndex.java` | **Create** | Rule tests |

---

## Phase 1: Grammar Rule

**File:** `presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4`

Add to the statement rule:

```antlr
statement
    : ...existing rules...
    | CREATE VECTOR INDEX identifier ON qualifiedName
        '(' identifier (',' identifier)* ')'
        (WHERE booleanExpression)?
        (WITH properties)?                                    #createVectorIndex
    ;
```

Add lexer tokens (REQUIRED - these do NOT exist in Presto):

```antlr
// Add to lexer tokens section (around line 800+)
VECTOR: 'VECTOR';
INDEX: 'INDEX';
```

**IMPORTANT:** Also add VECTOR and INDEX to the `nonReserved` rule to allow them as identifiers:

```antlr
nonReserved
    : ... existing tokens ...
    | VECTOR
    | INDEX
    ;
```

---

## Phase 2: CreateVectorIndex AST Node

**File:** `presto-parser/src/main/java/com/facebook/presto/sql/tree/CreateVectorIndex.java` (NEW)

```java
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateVectorIndex
        extends Statement
{
    private final Identifier indexName;
    private final QualifiedName tableName;
    private final List<Identifier> columns;
    private final Optional<Expression> where;
    private final List<Property> properties;

    public CreateVectorIndex(
            Identifier indexName,
            QualifiedName tableName,
            List<Identifier> columns,
            Optional<Expression> where,
            List<Property> properties)
    {
        this(Optional.empty(), indexName, tableName, columns, where, properties);
    }

    public CreateVectorIndex(
            NodeLocation location,
            Identifier indexName,
            QualifiedName tableName,
            List<Identifier> columns,
            Optional<Expression> where,
            List<Property> properties)
    {
        this(Optional.of(location), indexName, tableName, columns, where, properties);
    }

    private CreateVectorIndex(
            Optional<NodeLocation> location,
            Identifier indexName,
            QualifiedName tableName,
            List<Identifier> columns,
            Optional<Expression> where,
            List<Property> properties)
    {
        super(location);
        this.indexName = requireNonNull(indexName, "indexName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.where = requireNonNull(where, "where is null");
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
    }

    public Identifier getIndexName()
    {
        return indexName;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public List<Identifier> getColumns()
    {
        return columns;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateVectorIndex(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        children.add(indexName);
        children.addAll(columns);
        where.ifPresent(children::add);
        children.addAll(properties);
        return children.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexName, tableName, columns, where, properties);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        CreateVectorIndex o = (CreateVectorIndex) obj;
        return Objects.equals(indexName, o.indexName) &&
                Objects.equals(tableName, o.tableName) &&
                Objects.equals(columns, o.columns) &&
                Objects.equals(where, o.where) &&
                Objects.equals(properties, o.properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("indexName", indexName)
                .add("tableName", tableName)
                .add("columns", columns)
                .add("where", where)
                .add("properties", properties)
                .toString();
    }
}
```

---

## Phase 3: AstBuilder

**File:** `presto-parser/src/main/java/com/facebook/presto/sql/parser/AstBuilder.java`

```java
@Override
public Node visitCreateVectorIndex(SqlBaseParser.CreateVectorIndexContext context)
{
    Identifier indexName = (Identifier) visit(context.identifier(0));
    QualifiedName tableName = getQualifiedName(context.qualifiedName());

    // Columns start from identifier(1) onwards
    List<Identifier> columns = context.identifier().stream()
            .skip(1)  // Skip index name
            .map(id -> (Identifier) visit(id))
            .collect(toImmutableList());

    Optional<Expression> where = Optional.empty();
    if (context.WHERE() != null) {
        where = Optional.of((Expression) visit(context.booleanExpression()));
    }

    List<Property> properties = ImmutableList.of();
    if (context.properties() != null) {
        properties = visit(context.properties().property(), Property.class);
    }

    return new CreateVectorIndex(
            getLocation(context),
            indexName,
            tableName,
            columns,
            where,
            properties);
}
```

---

## Phase 4-6: Visitor Classes

### AstVisitor.java

**File:** `presto-parser/src/main/java/com/facebook/presto/sql/tree/AstVisitor.java`

```java
protected R visitCreateVectorIndex(CreateVectorIndex node, C context)
{
    return visitStatement(node, context);
}
```

### DefaultTraversalVisitor.java

**File:** `presto-parser/src/main/java/com/facebook/presto/sql/tree/DefaultTraversalVisitor.java`

```java
@Override
protected Void visitCreateVectorIndex(CreateVectorIndex node, C context)
{
    process(node.getIndexName(), context);
    for (Identifier column : node.getColumns()) {
        process(column, context);
    }
    node.getWhere().ifPresent(where -> process(where, context));
    for (Property property : node.getProperties()) {
        process(property, context);
    }
    return null;
}
```

### SqlFormatter.java

**File:** `presto-parser/src/main/java/com/facebook/presto/sql/SqlFormatter.java`

```java
@Override
protected Void visitCreateVectorIndex(CreateVectorIndex node, Integer indent)
{
    builder.append("CREATE VECTOR INDEX ");
    builder.append(formatName(node.getIndexName()));
    builder.append(" ON ");
    builder.append(formatName(node.getTableName()));
    builder.append(" (");
    builder.append(node.getColumns().stream()
            .map(SqlFormatter::formatName)
            .collect(joining(", ")));
    builder.append(")");

    node.getWhere().ifPresent(where -> {
        builder.append("\nWHERE ");
        builder.append(formatExpression(where, parameters));
    });

    if (!node.getProperties().isEmpty()) {
        builder.append("\nWITH (");
        builder.append(node.getProperties().stream()
                .map(property -> formatName(property.getName()) + " = " +
                        formatExpression(property.getValue(), parameters))
                .collect(joining(", ")));
        builder.append(")");
    }

    return null;
}
```

---

## Phase 7: StatementUtils - Query Type Registration

**File:** `presto-analyzer/src/main/java/com/facebook/presto/sql/analyzer/utils/StatementUtils.java`

Add import:
```java
import com.facebook.presto.sql.tree.CreateVectorIndex;
```

Add to the static block (around line 100, with other SELECT/INSERT registrations):
```java
// Register as SELECT to route through SqlQueryExecution (LogicalPlanner + Optimizer)
builder.put(CreateVectorIndex.class, QueryType.SELECT);
```

---

## Phase 8: StatementAnalyzer - Semantic Validation

**File:** `presto-main-base/src/main/java/com/facebook/presto/sql/analyzer/StatementAnalyzer.java`

```java
@Override
protected Scope visitCreateVectorIndex(CreateVectorIndex node, Optional<Scope> scope)
{
    // 1. Resolve table name
    QualifiedObjectName tableName = createQualifiedObjectName(session, node, node.getTableName());

    // 2. Verify table exists
    Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
    if (!tableHandle.isPresent()) {
        throw semanticException(TABLE_NOT_FOUND, node, "Table '%s' does not exist", tableName);
    }

    // 3. Resolve columns and verify they exist
    Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());
    Set<String> tableColumnNames = columnHandles.keySet().stream()
            .map(String::toLowerCase)
            .collect(toImmutableSet());

    for (Identifier column : node.getColumns()) {
        String columnName = column.getValue().toLowerCase();
        if (!tableColumnNames.contains(columnName)) {
            throw semanticException(COLUMN_NOT_FOUND, node,
                "Column '%s' does not exist in table '%s'", column.getValue(), tableName);
        }
    }

    // 4. Analyze WHERE clause if present (for syntax validation only)
    if (node.getWhere().isPresent()) {
        // Create scope with table columns for expression analysis
        Scope tableScope = createScopeForCommonTableExpression(node, scope, tableHandle.get());
        ExpressionAnalysis whereAnalysis = analyzeExpression(node.getWhere().get(), tableScope);
        // We don't need the result - just validating the expression is valid
    }

    // 5. Validate properties (optional - check for known property names)
    for (Property property : node.getProperties()) {
        // Could validate known properties: index_type, distance_metric, index_options
    }

    // 6. Store metadata in analysis for LogicalPlanner
    analysis.setCreateVectorIndexTableName(node, tableName);

    // 7. Return scope - output is a single BOOLEAN column
    // NOTE: Field.newUnqualified() requires NodeLocation as first parameter
    return createAndAssignScope(node, scope, Field.newUnqualified(node.getLocation(), "result", BOOLEAN));
}
```

---

## Phase 9: Analysis.java - Add Storage

**File:** `presto-main-base/src/main/java/com/facebook/presto/sql/analyzer/Analysis.java`

Add field:
```java
private final Map<NodeRef<CreateVectorIndex>, QualifiedObjectName> createVectorIndexTableNames = new LinkedHashMap<>();
```

Add methods:
```java
public void setCreateVectorIndexTableName(CreateVectorIndex node, QualifiedObjectName tableName)
{
    createVectorIndexTableNames.put(NodeRef.of(node), tableName);
}

public QualifiedObjectName getCreateVectorIndexTableName(CreateVectorIndex node)
{
    return createVectorIndexTableNames.get(NodeRef.of(node));
}
```

---

## Phase 10: CreateVectorIndexNode - Simplified Plan Node

**File:** `presto-main-base/src/main/java/com/facebook/presto/sql/planner/plan/CreateVectorIndexNode.java` (NEW)

This is a **simplified** plan node since we only store metadata (no TableHandle, no ColumnHandles):

```java
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Plan node representing CREATE VECTOR INDEX statement.
 *
 * This is a LEAF node with no sources. It stores metadata only:
 * - indexName, tableName, columns, whereClause (as SQL string), properties
 *
 * The RewriteCreateVectorIndex optimizer rule transforms this into:
 * ProjectNode(create_local_index(...)) -> ValuesNode
 */
@Immutable
public class CreateVectorIndexNode
        extends InternalPlanNode
{
    private final String indexName;
    private final String tableName;  // Fully qualified: catalog.schema.table
    private final List<String> columnNames;
    private final Optional<String> whereClause;  // SQL string, e.g., "ds = '2026-01-01'"
    private final Map<String, String> properties;
    private final VariableReferenceExpression outputVariable;

    @JsonCreator
    public CreateVectorIndexNode(
            @JsonProperty("sourceLocation") Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("indexName") String indexName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("whereClause") Optional<String> whereClause,
            @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("outputVariable") VariableReferenceExpression outputVariable)
    {
        // NOTE: InternalPlanNode requires 3 parameters (sourceLocation, id, statsEquivalentPlanNode)
        super(sourceLocation, id, Optional.empty());
        this.indexName = requireNonNull(indexName, "indexName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.whereClause = requireNonNull(whereClause, "whereClause is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.outputVariable = requireNonNull(outputVariable, "outputVariable is null");
    }

    @JsonProperty
    public String getIndexName()
    {
        return indexName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public Optional<String> getWhereClause()
    {
        return whereClause;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @JsonProperty
    public VariableReferenceExpression getOutputVariable()
    {
        return outputVariable;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();  // Leaf node - no sources
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.of(outputVariable);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "expected no children");
        return this;
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return this;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateVectorIndex(this, context);
    }
}
```

---

## Phase 11: InternalPlanVisitor

**File:** `presto-main-base/src/main/java/com/facebook/presto/sql/planner/plan/InternalPlanVisitor.java`

```java
public R visitCreateVectorIndex(CreateVectorIndexNode node, C context)
{
    return visitPlan(node, context);
}
```

---

## Phase 12: Patterns

**File:** `presto-main-base/src/main/java/com/facebook/presto/sql/planner/iterative/rule/Patterns.java`

```java
public static Pattern<CreateVectorIndexNode> createVectorIndex()
{
    return typeOf(CreateVectorIndexNode.class);
}
```

---

## Phase 13: LogicalPlanner

**File:** `presto-main-base/src/main/java/com/facebook/presto/sql/planner/LogicalPlanner.java`

Add to `planStatementWithoutOutput()`:
```java
else if (statement instanceof CreateVectorIndex) {
    return createVectorIndexPlan(analysis, (CreateVectorIndex) statement);
}
```

Add the planning method:
```java
private RelationPlan createVectorIndexPlan(Analysis analysis, CreateVectorIndex statement)
{
    // 1. Get fully qualified table name
    QualifiedObjectName tableName = analysis.getCreateVectorIndexTableName(statement);

    // 2. Extract column names as strings
    List<String> columnNames = statement.getColumns().stream()
            .map(Identifier::getValue)
            .collect(toImmutableList());

    // 3. Convert WHERE clause to SQL string using ExpressionFormatter (cleaner output)
    // Use ExpressionFormatter.formatExpression() for expressions, SqlFormatter.formatSql() for statements
    Optional<String> whereClause = statement.getWhere()
            .map(expr -> ExpressionFormatter.formatExpression(expr, Optional.empty()));

    // 4. Extract properties as Map<String, String>
    Map<String, String> properties = statement.getProperties().stream()
            .collect(toImmutableMap(
                    prop -> prop.getName().getValue(),
                    prop -> extractStringValue(prop.getValue())));

    // 5. Create output variable for function result
    VariableReferenceExpression outputVariable = variableAllocator.newVariable("result", BOOLEAN);

    // 6. Create CreateVectorIndexNode
    PlanNode node = new CreateVectorIndexNode(
            getSourceLocation(statement),
            idAllocator.getNextId(),
            statement.getIndexName().getValue(),
            tableName.toString(),
            columnNames,
            whereClause,
            properties,
            outputVariable);

    return new RelationPlan(node, analysis.getScope(statement), ImmutableList.of(outputVariable));
}

private String extractStringValue(Expression expression)
{
    if (expression instanceof StringLiteral) {
        return ((StringLiteral) expression).getValue();
    }
    return SqlFormatter.formatSql(expression, Optional.empty());
}
```

---

## Phase 14: RewriteCreateVectorIndex - Optimizer Rule

**File:** `presto-main-base/src/main/java/com/facebook/presto/sql/planner/iterative/rule/RewriteCreateVectorIndex.java` (NEW)

```java
package com.facebook.presto.sql.planner.iterative.rule;

// === COMPLETE IMPORT LIST ===
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.CreateVectorIndexNode;
import com.facebook.presto.sql.relational.Expressions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.plan.Patterns.createVectorIndex;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

// Additional imports for MapType and empty array/map handling:
import com.facebook.presto.metadata.CastType;  // NOT FunctionMetadata.CastType
import com.facebook.presto.common.block.Block;
import com.facebook.presto.sql.planner.PlannerUtils;  // For createMapType helper

// NOTE: Do NOT use io.delta.kernel.types.ArrayType or MapType - those are from Delta Kernel!
// NOTE: MapType requires 4 constructor args - use PlannerUtils.createMapType() instead

/**
 * Transforms CreateVectorIndexNode into:
 *
 * ProjectNode [create_local_index(table_name, columns, where_predicate, properties)]
 *   └─ ValuesNode (single empty row)
 *
 * The UDF receives metadata only and triggers a remote service to build the index.
 */
public class RewriteCreateVectorIndex
        implements Rule<CreateVectorIndexNode>
{
    private static final Pattern<CreateVectorIndexNode> PATTERN = createVectorIndex();
    private static final String UDF_NAME = "create_local_index";

    private final FunctionAndTypeManager functionAndTypeManager;

    public RewriteCreateVectorIndex(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public Pattern<CreateVectorIndexNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(CreateVectorIndexNode node, Captures captures, Context context)
    {
        // 1. Build ValuesNode with single empty row
        ValuesNode valuesNode = new ValuesNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                ImmutableList.of(),  // No output variables
                ImmutableList.of(ImmutableList.of()),  // Single empty row
                Optional.empty());

        // 2. Build create_local_index() function call
        CallExpression functionCall = buildCreateLocalIndexCall(node);

        // 3. Create ProjectNode with function call
        Assignments assignments = Assignments.builder()
                .put(node.getOutputVariable(), functionCall)
                .build();

        ProjectNode project = new ProjectNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                Optional.empty(),
                valuesNode,
                assignments,
                LOCAL);

        return Result.ofPlanNode(project);
    }

    /**
     * Builds the function call:
     * create_local_index(
     *     'table_name',           -- VARCHAR
     *     ARRAY['col1', 'col2'],  -- ARRAY(VARCHAR)
     *     'ds = ''2026-01-01''',  -- VARCHAR (nullable)
     *     MAP(ARRAY[...], ARRAY[...])  -- MAP(VARCHAR, VARCHAR)
     * )
     */
    private CallExpression buildCreateLocalIndexCall(CreateVectorIndexNode node)
    {
        List<RowExpression> args = new ArrayList<>();

        // 1. Table name as VARCHAR literal
        args.add(constant(utf8Slice(node.getTableName()), VARCHAR));

        // 2. Column names as ARRAY[VARCHAR]
        args.add(buildStringArray(node.getColumnNames()));

        // 3. WHERE clause as VARCHAR literal (or NULL if not present)
        if (node.getWhereClause().isPresent()) {
            args.add(constant(Slices.utf8Slice(node.getWhereClause().get()), VARCHAR));
        }
        else {
            args.add(constantNull(VARCHAR));
        }

        // 4. Properties as MAP(VARCHAR, VARCHAR)
        args.add(buildPropertiesMap(node.getProperties()));

        // Build the function call
        FunctionHandle functionHandle = functionAndTypeManager.lookupFunction(
                UDF_NAME,
                fromTypes(ImmutableList.of(
                        VARCHAR,                    // table_name
                        new ArrayType(VARCHAR),     // columns
                        VARCHAR,                    // where_predicate
                        new MapType(VARCHAR, VARCHAR)  // properties
                )));

        return Expressions.call(
                UDF_NAME,
                functionHandle,
                BOOLEAN,
                args);
    }

    /**
     * Builds ARRAY['val1', 'val2', ...] expression
     *
     * NOTE: For empty arrays, we create an empty Block constant since:
     * - $array with 0 arguments won't match any function signature
     * - SpecialFormExpression.Form.ARRAY_CONSTRUCTOR does NOT exist
     * - Expressions.specialForm() method does NOT exist
     */
    private RowExpression buildStringArray(List<String> values)
    {
        ArrayType arrayType = new ArrayType(VARCHAR);

        if (values.isEmpty()) {
            // For empty arrays, create an empty Block constant directly
            // This avoids issues with function lookup for 0-argument arrays
            Block emptyBlock = arrayType.createBlockBuilder(null, 0).build();
            return constant(emptyBlock, arrayType);
        }

        List<RowExpression> elements = values.stream()
                .map(v -> constant(utf8Slice(v), VARCHAR))
                .collect(toImmutableList());

        // Use array_constructor function (internal name is "$array")
        // Lookup with the actual element types - must have at least 1 element
        FunctionHandle arrayConstructor = functionAndTypeManager.lookupFunction(
                "array_constructor",
                fromTypes(elements.stream().map(e -> VARCHAR).collect(toImmutableList())));

        return Expressions.call(
                "array_constructor",
                arrayConstructor,
                arrayType,
                elements);
    }

    /**
     * Builds MAP(ARRAY['key1', ...], ARRAY['val1', ...]) expression
     */
    /**
     * Builds MAP(ARRAY['key1', ...], ARRAY['val1', ...]) expression
     *
     * NOTE: MapType constructor requires 4 parameters, NOT 2!
     * Use PlannerUtils.createMapType() helper instead of direct construction.
     */
    private RowExpression buildPropertiesMap(Map<String, String> properties)
    {
        // Get properly constructed MapType via helper (NOT new MapType(VARCHAR, VARCHAR)!)
        // MapType constructor signature: MapType(Type keyType, Type valueType, MethodHandle keyBlockEquals, MethodHandle keyBlockHashCode)
        Type mapType = PlannerUtils.createMapType(functionAndTypeManager, VARCHAR, VARCHAR);

        if (properties.isEmpty()) {
            // For empty maps, create an empty Block constant directly
            Block emptyMapBlock = mapType.createBlockBuilder(null, 0).build();
            return constant(emptyMapBlock, mapType);
        }

        List<String> keys = new ArrayList<>(properties.keySet());
        List<String> values = keys.stream()
                .map(properties::get)
                .collect(toImmutableList());

        RowExpression keysArray = buildStringArray(keys);
        RowExpression valuesArray = buildStringArray(values);

        FunctionHandle mapFunction = functionAndTypeManager.lookupFunction(
                "map",
                fromTypes(ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR))));

        return Expressions.call(
                "map",
                mapFunction,
                mapType,
                ImmutableList.of(keysArray, valuesArray));
    }
}
```

---

## Phase 15: Register Rule in PlanOptimizers

**File:** `presto-main-base/src/main/java/com/facebook/presto/sql/planner/PlanOptimizers.java`

Add the rule early in the logical optimization phase:

```java
// Early in the logical optimization phase - rewrite CREATE VECTOR INDEX
builder.add(new IterativeOptimizer(
        metadata,
        ruleStats,
        statsCalculator,
        estimatedExchangesCostCalculator,
        ImmutableSet.of(new RewriteCreateVectorIndex(metadata.getFunctionAndTypeManager()))));
```

---

## Phase 16: Python UDF Registration

The `create_local_index` function needs to be registered.

### UDF Signature

```sql
CREATE FUNCTION create_local_index(
    table_name VARCHAR,
    columns ARRAY(VARCHAR),
    where_predicate VARCHAR,  -- Can be NULL
    properties MAP(VARCHAR, VARCHAR)
)
RETURNS BOOLEAN
LANGUAGE PYTHON
EXTERNAL;
```

### UDF Behavior

The Python UDF:
1. Receives metadata (table name, columns, WHERE string, properties)
2. Calls a remote service with this metadata
3. The remote service:
   - Executes a query to fetch data from the table with the WHERE clause
   - Builds the vector index
   - Stores the index
4. Returns TRUE on success, FALSE on failure

---

## Testing

### Parser Tests

**File:** `presto-parser/src/test/java/com/facebook/presto/sql/parser/TestSqlParser.java`

```java
@Test
public void testCreateVectorIndex()
{
    assertStatement(
            "CREATE VECTOR INDEX idx ON t(a, b)",
            new CreateVectorIndex(
                    identifier("idx"),
                    QualifiedName.of("t"),
                    ImmutableList.of(identifier("a"), identifier("b")),
                    Optional.empty(),
                    ImmutableList.of()));

    assertStatement(
            "CREATE VECTOR INDEX idx ON schema.table(id, embedding) WHERE ds = '2024-01-01'",
            new CreateVectorIndex(
                    identifier("idx"),
                    QualifiedName.of("schema", "table"),
                    ImmutableList.of(identifier("id"), identifier("embedding")),
                    Optional.of(new ComparisonExpression(
                            EQUAL,
                            new Identifier("ds"),
                            new StringLiteral("2024-01-01"))),
                    ImmutableList.of()));

    assertStatement(
            "CREATE VECTOR INDEX idx ON t(c) WITH (index_type = 'ivf', metric = 'cosine')",
            new CreateVectorIndex(
                    identifier("idx"),
                    QualifiedName.of("t"),
                    ImmutableList.of(identifier("c")),
                    Optional.empty(),
                    ImmutableList.of(
                            new Property(identifier("index_type"), new StringLiteral("ivf")),
                            new Property(identifier("metric"), new StringLiteral("cosine")))));
}
```

### Optimizer Rule Tests

**File:** `presto-main-base/src/test/java/com/facebook/presto/sql/planner/iterative/rule/TestRewriteCreateVectorIndex.java` (NEW)

```java
@Test
public void testBasicRewrite()
{
    // Verify CreateVectorIndexNode → ProjectNode(ValuesNode) transformation
    tester().assertThat(new RewriteCreateVectorIndex(tester().getMetadata().getFunctionAndTypeManager()))
            .on(p -> new CreateVectorIndexNode(
                    Optional.empty(),
                    p.nextId(),
                    "idx",
                    "catalog.schema.table",
                    ImmutableList.of("id", "embedding"),
                    Optional.empty(),
                    ImmutableMap.of(),
                    p.variable("result", BOOLEAN)))
            .matches(
                    project(
                            ImmutableMap.of("result", expression("create_local_index(...)")),
                            values()));
}

@Test
public void testRewriteWithWhereClause()
{
    tester().assertThat(new RewriteCreateVectorIndex(tester().getMetadata().getFunctionAndTypeManager()))
            .on(p -> new CreateVectorIndexNode(
                    Optional.empty(),
                    p.nextId(),
                    "idx",
                    "t",
                    ImmutableList.of("c"),
                    Optional.of("ds = '2024-01-01'"),
                    ImmutableMap.of(),
                    p.variable("result", BOOLEAN)))
            .matches(
                    project(
                            values()));
}

@Test
public void testRewriteWithProperties()
{
    tester().assertThat(new RewriteCreateVectorIndex(tester().getMetadata().getFunctionAndTypeManager()))
            .on(p -> new CreateVectorIndexNode(
                    Optional.empty(),
                    p.nextId(),
                    "idx",
                    "t",
                    ImmutableList.of("c"),
                    Optional.empty(),
                    ImmutableMap.of("index_type", "ivf", "metric", "cosine"),
                    p.variable("result", BOOLEAN)))
            .matches(
                    project(
                            values()));
}
```

---

## Build and Test Commands

```bash
# Navigate to presto directory
cd /data/sandcastle/boxes/fbsource/fbcode/github/presto-trunk

# Regenerate ANTLR parser
cd presto-parser && mvn generate-sources

# Run parser tests
mvn test -pl presto-parser -Dtest=TestSqlParser#testCreateVectorIndex

# Run optimizer rule tests
mvn test -pl presto-main-base -Dtest=TestRewriteCreateVectorIndex

# Full build
mvn clean install -DskipTests
mvn verify
```

---

## Implementation Checklist

### Parser Module (`presto-parser`)
- [ ] Add grammar rule to `SqlBase.g4`
- [ ] Create `CreateVectorIndex.java` AST node
- [ ] Add `visitCreateVectorIndex()` to `AstBuilder.java`
- [ ] Add `visitCreateVectorIndex()` to `AstVisitor.java`
- [ ] Add `visitCreateVectorIndex()` to `DefaultTraversalVisitor.java`
- [ ] Add `visitCreateVectorIndex()` to `SqlFormatter.java`
- [ ] Add parser tests

### Analyzer Module (`presto-analyzer`)
- [ ] Add `CreateVectorIndex` to `StatementUtils.java` as `QueryType.SELECT`

### Analyzer Module (`presto-main-base`)
- [ ] Add `visitCreateVectorIndex()` to `StatementAnalyzer.java`
- [ ] Add storage methods to `Analysis.java`

### Planner Module (`presto-main-base`)
- [ ] Create `CreateVectorIndexNode.java` plan node
- [ ] Add `visitCreateVectorIndex()` to `InternalPlanVisitor.java`
- [ ] Add `createVectorIndex()` pattern to `Patterns.java`
- [ ] Add `createVectorIndexPlan()` to `LogicalPlanner.java`

### Optimizer Module (`presto-main-base`)
- [ ] Create `RewriteCreateVectorIndex.java` optimizer rule
- [ ] Register rule in `PlanOptimizers.java`
- [ ] Add optimizer rule tests

### Python UDF
- [ ] Create Python UDF implementation
- [ ] Register UDF (CREATE FUNCTION or JSON signature)

---

## Summary

This implementation:

1. **Parses** `CREATE VECTOR INDEX` to an AST node
2. **Routes** through query execution (not DDL) by registering as `QueryType.SELECT`
3. **Validates** table and columns exist in `StatementAnalyzer`
4. **Creates** a `CreateVectorIndexNode` leaf plan node with metadata
5. **Transforms** via optimizer rule to `ProjectNode(ValuesNode)` calling `create_local_index` UDF
6. **Executes** the UDF once, which triggers a remote service with metadata

The UDF signature is simple and fixed:
```
create_local_index(table_name, columns, where_predicate, properties) → BOOLEAN
```

The remote service handles all data access and index building.
