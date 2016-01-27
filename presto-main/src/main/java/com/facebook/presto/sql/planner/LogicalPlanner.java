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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NewTableLayout;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.MaterializedQueryTableInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableIdentity;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.sanity.PlanSanityChecker;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.CreateName;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.InsertReference;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.WriterTarget;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LogicalPlanner
{
    public enum Stage
    {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    private final PlanNodeIdAllocator idAllocator;

    private final Session session;
    private final List<PlanOptimizer> planOptimizers;
    private final SymbolAllocator symbolAllocator = new SymbolAllocator();
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public LogicalPlanner(Session session,
            List<PlanOptimizer> planOptimizers,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            SqlParser sqlParser)
    {
        requireNonNull(session, "session is null");
        requireNonNull(planOptimizers, "planOptimizers is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlParser, "sqlParser is null");

        this.session = session;
        this.planOptimizers = planOptimizers;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.sqlParser = sqlParser;
    }

    public Plan plan(Analysis analysis)
    {
        return plan(analysis, Stage.OPTIMIZED_AND_VALIDATED);
    }

    public Plan plan(Analysis analysis, Stage stage)
    {
        PlanNode root = planStatement(analysis, analysis.getStatement());

        if (stage.ordinal() >= Stage.OPTIMIZED.ordinal()) {
            for (PlanOptimizer optimizer : planOptimizers) {
                root = optimizer.optimize(root, session, symbolAllocator.getTypes(), symbolAllocator, idAllocator);
                requireNonNull(root, format("%s returned a null plan", optimizer.getClass().getName()));
            }
        }

        if (stage.ordinal() >= Stage.OPTIMIZED_AND_VALIDATED.ordinal()) {
            // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
            PlanSanityChecker.validate(root, session, metadata, sqlParser, symbolAllocator.getTypes());
        }

        return new Plan(root, symbolAllocator);
    }

    public PlanNode planStatement(Analysis analysis, Statement statement)
    {
        if (statement instanceof CreateTableAsSelect) {
            checkState(analysis.getCreateTableDestination().isPresent(), "Table destination is missing");
            if (analysis.isCreateTableAsSelectNoOp()) {
                List<Expression> emptyRow = ImmutableList.of();
                PlanNode source = new ValuesNode(idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of(emptyRow));
                return new OutputNode(idAllocator.getNextId(), source, ImmutableList.of(), ImmutableList.of());
            }
            else {
                return createOutputPlan(createTableCreationPlan(analysis, ((CreateTableAsSelect) statement).getQuery()), analysis);
            }
        }
        else if (statement instanceof Insert) {
            checkState(analysis.getInsert().isPresent(), "Insert handle is missing");
            return createOutputPlan(createInsertPlan(analysis, (Insert) statement), analysis);
        }
        else if (statement instanceof Delete) {
            return createOutputPlan(createDeletePlan(analysis, (Delete) statement), analysis);
        }
        else if (statement instanceof Query) {
            return createOutputPlan(createRelationPlan(analysis, (Query) statement), analysis);
        }
        else if (statement instanceof Explain && ((Explain) statement).isAnalyze()) {
            return createOutputPlan(createExplainAnalyzePlan(analysis, (Explain) statement), analysis);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type " + statement.getClass().getSimpleName());
        }
    }

    private RelationPlan createExplainAnalyzePlan(Analysis analysis, Explain statement)
    {
        Scope scope = analysis.getScope(statement);
        PlanNode root = planStatement(analysis, statement.getStatement());
        Symbol outputSymbol = symbolAllocator.newSymbol(scope.getRelationType().getFieldByIndex(0));
        root = new ExplainAnalyzeNode(idAllocator.getNextId(), root, outputSymbol);
        return new RelationPlan(root, scope, ImmutableList.of(outputSymbol), Optional.empty());
    }

    private RelationPlan createTableCreationPlan(Analysis analysis, Query query)
    {
        QualifiedObjectName destination = analysis.getCreateTableDestination().get();

        RelationPlan plan = createRelationPlan(analysis, query);
        Optional<MaterializedQueryTableInfo> materializedQueryTableInfo = Optional.empty();
        if (analysis.isCreateMaterializedQueryTable()) {
            materializedQueryTableInfo = Optional.of(getMaterializedQueryTableInfo(analysis, ((CreateTableAsSelect) analysis.getStatement()).getQuery(), session));
        }

        ConnectorTableMetadata tableMetadata = createTableMetadata(
                destination,
                getOutputTableColumns(plan),
                analysis.getCreateTableProperties(),
                plan.getSampleWeight().isPresent(),
                analysis.getParameters(),
                materializedQueryTableInfo);

        if (plan.getSampleWeight().isPresent() && !metadata.canCreateSampledTables(session, destination.getCatalogName())) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot write sampled data to a store that doesn't support sampling");
        }

        Optional<NewTableLayout> newTableLayout = metadata.getNewTableLayout(session, destination.getCatalogName(), tableMetadata);

        List<String> columnNames = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        return createTableWriterPlan(
                analysis,
                plan,
                new CreateName(destination.getCatalogName(), tableMetadata, newTableLayout),
                columnNames,
                newTableLayout);
    }

    private MaterializedQueryTableInfo getMaterializedQueryTableInfo(Analysis analysis, Query query, Session session)
    {
        checkState(session.getCatalog().isPresent(), "catalog is not present in session");
        checkState(session.getSchema().isPresent(), "schema is not present in session");

        StringBuilder stringBuilder = new StringBuilder();
        SqlFormatter.Formatter formatter = new SqlFormatter.Formatter(stringBuilder, false, Optional.empty())
        {
            @Override
            protected Void visitTable(Table node, Integer indent)
            {
                QualifiedName qualifiedName = node.getName();
                if (qualifiedName.getParts().size() == 1) {
                    qualifiedName = QualifiedName.of(session.getCatalog().get(), session.getSchema().get(), qualifiedName.getSuffix());
                }
                else if (qualifiedName.getParts().size() == 2) {
                    qualifiedName = QualifiedName.of(session.getCatalog().get(), qualifiedName.getPrefix().get().toString(), qualifiedName.getSuffix());
                }
                stringBuilder.append(qualifiedName.toString());
                return null;
            }

            @Override
            protected Void visitAllColumns(AllColumns node, Integer context)
            {
                throw new PrestoException(NOT_SUPPORTED, "Select * is not supported in materialized query table definition");
            }
        };

        formatter.process(query, 0);
        String queryStr = stringBuilder.toString();

        Map<QualifiedObjectName, List<String>> usedColumns = analysis.getTableColumns();
        Map<String, byte[]> tableIdentities = new HashMap<>();
        Map<String, Map<String, byte[]>> columnIdentities = new HashMap<>();
        for (QualifiedObjectName tableName : usedColumns.keySet()) {
            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
            checkState(tableHandle.isPresent(), "tableHandle for %s is null", tableName);
            TableIdentity tableIdentity = metadata.getTableIdentity(session, tableHandle.get());

            Map<String, byte[]> columnIdentitiesMap = metadata.getColumnHandles(session, tableHandle.get()).entrySet().stream()
                    .filter(entry -> usedColumns.get(tableName).contains(entry.getKey()))
                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> metadata.getColumnIdentity(session, tableHandle.get(), entry.getValue()).serialize()));

            checkState(columnIdentitiesMap.size() == usedColumns.get(tableName).size(), "Missing columns for table %s", tableName);
            tableIdentities.put(tableName.toString(), tableIdentity.serialize());
            columnIdentities.put(tableName.toString(), columnIdentitiesMap);
        }

        return new MaterializedQueryTableInfo(queryStr, tableIdentities, columnIdentities, MaterializedQueryTableInfo.DEFAULT_REFRESH_TIMESTAMP);
    }

    private RelationPlan createInsertPlan(Analysis analysis, Insert insertStatement)
    {
        Analysis.Insert insert = analysis.getInsert().get();

        TableMetadata tableMetadata = metadata.getTableMetadata(session, insert.getTarget());

        List<ColumnMetadata> visibleTableColumns = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .collect(toImmutableList());
        List<String> visibleTableColumnNames = visibleTableColumns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        RelationPlan plan = createRelationPlan(analysis, insertStatement.getQuery());

        Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, insert.getTarget());
        ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.isHidden()) {
                continue;
            }
            Symbol output = symbolAllocator.newSymbol(column.getName(), column.getType());
            int index = insert.getColumns().indexOf(columns.get(column.getName()));
            if (index < 0) {
                assignments.put(output, new NullLiteral());
            }
            else {
                Symbol input = plan.getSymbol(index);
                Type tableType = column.getType();
                Type queryType = symbolAllocator.getTypes().get(input);

                if (queryType.equals(tableType) || metadata.getTypeManager().isTypeOnlyCoercion(queryType, tableType)) {
                    assignments.put(output, input.toSymbolReference());
                }
                else {
                    Expression cast = new Cast(input.toSymbolReference(), tableType.getTypeSignature().toString());
                    assignments.put(output, cast);
                }
            }
        }
        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());

        List<Field> fields = visibleTableColumns.stream()
                .map(column -> Field.newUnqualified(column.getName(), column.getType()))
                .collect(toImmutableList());
        Scope scope = Scope.builder().withRelationType(new RelationType(fields)).build();

        plan = new RelationPlan(
                projectNode,
                scope,
                projectNode.getOutputSymbols(),
                plan.getSampleWeight());

        Optional<NewTableLayout> newTableLayout = metadata.getInsertLayout(session, insert.getTarget());

        return createTableWriterPlan(
                analysis,
                plan,
                new InsertReference(insert.getTarget()),
                visibleTableColumnNames,
                newTableLayout);
    }

    private RelationPlan createTableWriterPlan(
            Analysis analysis,
            RelationPlan plan,
            WriterTarget target,
            List<String> columnNames,
            Optional<NewTableLayout> writeTableLayout)
    {
        List<Symbol> writerOutputs = ImmutableList.of(
                symbolAllocator.newSymbol("partialrows", BIGINT),
                symbolAllocator.newSymbol("fragment", VARBINARY));

        PlanNode source = plan.getRoot();

        if (!analysis.isCreateTableAsSelectWithData()) {
            source = new LimitNode(idAllocator.getNextId(), source, 0L, false);
        }

        // todo this should be checked in analysis
        writeTableLayout.ifPresent(layout -> {
            if (!ImmutableSet.copyOf(columnNames).containsAll(layout.getPartitionColumns())) {
                throw new PrestoException(NOT_SUPPORTED, "INSERT must write all distribution columns: " + layout.getPartitionColumns());
            }
        });

        List<Symbol> symbols = plan.getOutputSymbols();

        Optional<PartitioningScheme> partitioningScheme = Optional.empty();
        if (writeTableLayout.isPresent()) {
            List<Symbol> partitionFunctionArguments = new ArrayList<>();
            writeTableLayout.get().getPartitionColumns().stream()
                    .mapToInt(columnNames::indexOf)
                    .mapToObj(symbols::get)
                    .forEach(partitionFunctionArguments::add);
            plan.getSampleWeight()
                    .ifPresent(partitionFunctionArguments::add);

            List<Symbol> outputLayout = new ArrayList<>(symbols);
            plan.getSampleWeight()
                    .ifPresent(outputLayout::add);

            partitioningScheme = Optional.of(new PartitioningScheme(
                    Partitioning.create(writeTableLayout.get().getPartitioning(), partitionFunctionArguments),
                    outputLayout));
        }

        PlanNode writerNode = new TableWriterNode(
                idAllocator.getNextId(),
                source,
                target,
                symbols,
                columnNames,
                writerOutputs,
                plan.getSampleWeight(),
                partitioningScheme);

        List<Symbol> outputs = ImmutableList.of(symbolAllocator.newSymbol("rows", BIGINT));
        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                writerNode,
                target,
                outputs);

        return new RelationPlan(commitNode, analysis.getRootScope(), outputs, Optional.empty());
    }

    private RelationPlan createDeletePlan(Analysis analysis, Delete node)
    {
        QueryPlanner planner = new QueryPlanner(analysis, symbolAllocator, idAllocator, metadata, session, Optional.empty());
        DeleteNode deleteNode = planner.plan(node);

        List<Symbol> outputs = ImmutableList.of(symbolAllocator.newSymbol("rows", BIGINT));
        TableFinishNode commitNode = new TableFinishNode(idAllocator.getNextId(), deleteNode, deleteNode.getTarget(), outputs);

        return new RelationPlan(commitNode, analysis.getScope(node), commitNode.getOutputSymbols(), Optional.empty());
    }

    private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis)
    {
        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();

        int columnNumber = 0;
        RelationType outputDescriptor = analysis.getOutputDescriptor();
        for (Field field : outputDescriptor.getVisibleFields()) {
            String name = field.getName().orElse("_col" + columnNumber);
            names.add(name);

            int fieldIndex = outputDescriptor.indexOf(field);
            Symbol symbol = plan.getSymbol(fieldIndex);
            outputs.add(symbol);

            columnNumber++;
        }

        return new OutputNode(idAllocator.getNextId(), plan.getRoot(), names.build(), outputs.build());
    }

    private RelationPlan createRelationPlan(Analysis analysis, Query query)
    {
        return new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session)
                .process(query, null);
    }

    private ConnectorTableMetadata createTableMetadata(
            QualifiedObjectName table,
            List<ColumnMetadata> columns,
            Map<String, Expression> propertyExpressions,
            boolean sampled,
            List<Expression> parameters,
            Optional<MaterializedQueryTableInfo> materializedQueryTableInfo)
    {
        Map<String, Object> properties = metadata.getTablePropertyManager().getProperties(
                table.getCatalogName(),
                propertyExpressions,
                session,
                metadata,
                parameters);

        return new ConnectorTableMetadata(table.asSchemaTableName(), columns, properties, sampled, materializedQueryTableInfo);
    }

    private static List<ColumnMetadata> getOutputTableColumns(RelationPlan plan)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (Field field : plan.getDescriptor().getVisibleFields()) {
            columns.add(new ColumnMetadata(field.getName().get(), field.getType()));
        }
        return columns.build();
    }
}
