package com.facebook.presto.sql.planner;

import com.facebook.presto.importer.PeriodicImportJob;
import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.storage.StorageManager;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkState;

public class LogicalPlanner
{
    private final PlanNodeIdAllocator idAllocator;

    private final Session session;
    private final List<PlanOptimizer> planOptimizers;
    private final SymbolAllocator symbolAllocator = new SymbolAllocator();

    // for materialized views
    private final Metadata metadata;
    private final PeriodicImportManager periodicImportManager;
    private final StorageManager storageManager;

    public LogicalPlanner(Session session,
            List<PlanOptimizer> planOptimizers,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            PeriodicImportManager periodicImportManager,
            StorageManager storageManager)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(planOptimizers, "planOptimizers is null");
        Preconditions.checkNotNull(idAllocator, "idAllocator is null");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(periodicImportManager, "periodicImportManager is null");
        Preconditions.checkNotNull(storageManager, "storageManager is null");

        this.session = session;
        this.planOptimizers = planOptimizers;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.periodicImportManager = periodicImportManager;
        this.storageManager = storageManager;
    }

    public Plan plan(Analysis analysis)
    {
        RelationPlan plan;
        if (analysis.getDestination() != null) {
            plan = createTableWriterPlan(analysis);
        }
        else {
            RelationPlanner planner = new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session);
            plan = planner.process(analysis.getQuery(), null);
        }

        PlanNode root = createOutputPlan(plan, analysis);

        // make sure we produce a valid plan. This is mainly to catch programming errors
        PlanSanityChecker.validate(root);

        for (PlanOptimizer optimizer : planOptimizers) {
            root = optimizer.optimize(root, session, symbolAllocator.getTypes(), idAllocator);
        }

        // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
        PlanSanityChecker.validate(root);

        return new Plan(root, symbolAllocator);
    }

    private RelationPlan createTableWriterPlan(Analysis analysis)
    {
        QualifiedTableName destination = analysis.getDestination();

        TableHandle targetTable;
        List<ColumnHandle> columnHandles;

        RelationPlan plan;
        if (analysis.isDoRefresh()) {
            // TODO: some of this should probably move to the analyzer, which should be able to compute the tuple descriptor for the source table from metadata
            targetTable = metadata.getTableHandle(destination).get();
            QualifiedTableName sourceTable = storageManager.getTableSource((NativeTableHandle) targetTable);
            TableHandle sourceTableHandle = metadata.getTableHandle(sourceTable).get();
            TableMetadata sourceTableMetadata = metadata.getTableMetadata(sourceTableHandle);
            Map<String,ColumnHandle> sourceTableColumns = metadata.getColumnHandles(sourceTableHandle);

            ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            ImmutableList.Builder<ColumnHandle> columnHandleBuilder = ImmutableList.builder();

            for (ColumnMetadata column : sourceTableMetadata.getColumns()) {
                Field field = Field.newQualified(sourceTable.asQualifiedName(), Optional.of(column.getName()), Type.fromRaw(column.getType()));
                Symbol symbol = symbolAllocator.newSymbol(field);
                ColumnHandle columnHandle = sourceTableColumns.get(column.getName());

                columns.put(symbol, columnHandle);
                fields.add(field);
                columnHandleBuilder.add(columnHandle);
                outputSymbolsBuilder.add(symbol);
            }

            ImmutableList<Symbol> outputSymbols = outputSymbolsBuilder.build();
            plan = new RelationPlan(new TableScanNode(idAllocator.getNextId(), sourceTableHandle, outputSymbols, columns.build(), TRUE_LITERAL, TRUE_LITERAL), new TupleDescriptor(fields.build()), outputSymbols);

            columnHandles = columnHandleBuilder.build();
        }
        else {
            RelationPlanner planner = new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session);
            plan = planner.process(analysis.getQuery(), null);

            // TODO: create table and periodic import in pre-execution step, not here

            // Create the destination table
            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            for (int i = 0; i < plan.getDescriptor().getFields().size(); i++) {
                Field field = plan.getDescriptor().getFields().get(i);
                String name = field.getName().or("_field" + i);
                ColumnMetadata columnMetadata = new ColumnMetadata(name, field.getType().getColumnType(), i, false);
                columns.add(columnMetadata);
            }
            TableMetadata tableMetadata = new TableMetadata(destination.asSchemaTableName(), columns.build());
            targetTable = metadata.createTable(destination.getCatalogName(), tableMetadata);

            // get the column handles for the destination table
            Map<String, ColumnHandle> columnHandleIndex = metadata.getColumnHandles(targetTable);
            ImmutableList.Builder<ColumnHandle> columnHandleBuilder = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                columnHandleBuilder.add(columnHandleIndex.get(column.getName()));
            }
            columnHandles = columnHandleBuilder.build();

            // find source table (TODO: do this in analyzer)
            QueryBody queryBody = analysis.getQuery().getQueryBody();
            checkState(queryBody instanceof QuerySpecification, "Query is not a simple select statement");
            List<Relation> relations = ((QuerySpecification) queryBody).getFrom();
            checkState(relations.size() == 1, "Query has more than one source table");
            Relation relation = Iterables.getOnlyElement(relations);
            checkState(relation instanceof Table, "FROM clause is not a simple table name");
            QualifiedTableName sourceTable = MetadataUtil.createQualifiedTableName(session, ((Table) relation).getName());


            // create source table and optional import information
            storageManager.insertTableSource(((NativeTableHandle) targetTable), sourceTable);

            // if a refresh is present, create a periodic import for this table
            if (analysis.getRefreshInterval().isPresent()) {
                PeriodicImportJob job = PeriodicImportJob.createJob(sourceTable, destination, analysis.getRefreshInterval().get());
                periodicImportManager.insertJob(job);
            }
        }

        // compute input symbol <-> column mappings
        ImmutableMap.Builder<Symbol, ColumnHandle> mappings = ImmutableMap.builder();

        for (int i = 0; i < columnHandles.size(); i++) {
            ColumnHandle column = columnHandles.get(i);
            Symbol symbol = plan.getSymbol(i);
            mappings.put(symbol, column);
        }

        // create writer node
        Symbol output = symbolAllocator.newSymbol("imported_rows", Type.LONG);

        TableWriterNode writerNode = new TableWriterNode(idAllocator.getNextId(),
                plan.getRoot(),
                targetTable,
                mappings.build(),
                output);

        return new RelationPlan(writerNode, analysis.getOutputDescriptor(), ImmutableList.of(output));
    }

    private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis)
    {
        ImmutableList.Builder<String> names = ImmutableList.builder();
        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();

        for (int i = 0; i < analysis.getOutputDescriptor().getFields().size(); i++) {
            Field field = analysis.getOutputDescriptor().getFields().get(i);
            String name = field.getName().or("_col" + i);

            names.add(name);
            outputs.add(plan.getSymbol(i));
        }

        return new OutputNode(idAllocator.getNextId(), plan.getRoot(), names.build(), outputs.build());
    }
}
