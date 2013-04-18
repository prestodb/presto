package com.facebook.presto.sql.planner;

import com.facebook.presto.importer.PeriodicImportJob;
import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
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
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.storage.StorageManager;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.metadata.ColumnMetadata.columnHandleGetter;
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
            RelationPlanner planner = new RelationPlanner(analysis, symbolAllocator, idAllocator);
            plan = planner.process(analysis.getQuery(), null);
        }

        PlanNode root = createOutputPlan(plan, analysis);

        // make sure we produce a valid plan. This is mainly to catch programming errors
        PlanSanityChecker.validate(root);

        for (PlanOptimizer optimizer : planOptimizers) {
            root = optimizer.optimize(root, session, symbolAllocator.getTypes());
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
        TupleDescriptor inputDescriptor;

        RelationPlan plan;
        if (analysis.isDoRefresh()) {
            // TODO: some of this should probably move to the analyzer, which should be able to compute the tuple descriptor for the source table from metadata
            targetTable = metadata.getTable(destination).getTableHandle().get();
            QualifiedTableName sourceTable = storageManager.getTableSource((NativeTableHandle) targetTable);
            TableMetadata sourceTableMetadata = metadata.getTable(sourceTable);
            TableHandle sourceTableHandle = sourceTableMetadata.getTableHandle().get();

            ImmutableMap.Builder<Field, Symbol> mappings = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            ImmutableList.Builder<ColumnHandle> columnHandleBuilder = ImmutableList.builder();

            int index = 0;
            for (ColumnMetadata column : sourceTableMetadata.getColumns()) {
                Field field = new Field(sourceTable.asQualifiedName(), Optional.of(column.getName()), Type.fromRaw(column.getType()), index++);
                Symbol symbol = symbolAllocator.newSymbol(field);
                ColumnHandle columnHandle = column.getColumnHandle().get();

                columns.put(symbol, columnHandle);
                fields.add(field);
                columnHandleBuilder.add(columnHandle);
            }

            plan = new RelationPlan(mappings.build(), new TableScanNode(idAllocator.getNextId(), sourceTableHandle, columns.build()));
            columnHandles = columnHandleBuilder.build();
            inputDescriptor = new TupleDescriptor(fields.build());
        }
        else {
            RelationPlanner planner = new RelationPlanner(analysis, symbolAllocator, idAllocator);
            plan = planner.process(analysis.getQuery(), null);

            inputDescriptor = analysis.getOutputDescriptor(analysis.getQuery());

            // TODO: create table and periodic import in pre-execution step, not here

            // Create the destination table
            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            for (Field field : inputDescriptor.getFields()) {
                String name = field.getName().or("_col" + field.getIndex());
                ColumnMetadata columnMetadata = new ColumnMetadata(name, field.getType().getRawType());
                columns.add(columnMetadata);
            }

            metadata.createTable(new TableMetadata(destination, columns.build()));
            TableMetadata tableMetadata = metadata.getTable(destination);
            targetTable = tableMetadata.getTableHandle().get();

            columnHandles = Lists.transform(tableMetadata.getColumns(), columnHandleGetter());

            // find source table (TODO: do this in analyzer)
            List<Relation> relations = analysis.getQuery().getFrom();
            checkState(relations.size() == 1, "Query has more than one source table");
            Relation relation = Iterables.getOnlyElement(relations);
            checkState(relation instanceof Table, "FROM clause is not a simple table name");
            QualifiedTableName sourceTable = MetadataUtil.createQualifiedTableName(session, ((Table) relation).getName());


            // create source table and optional import information
            storageManager.insertTableSource(((NativeTableHandle) tableMetadata.getTableHandle().get()), sourceTable);

            // if a refresh is present, create a periodic import for this table
            if (analysis.getRefreshInterval().isPresent()) {
                PeriodicImportJob job = PeriodicImportJob.createJob(sourceTable, destination, analysis.getRefreshInterval().get());
                periodicImportManager.insertJob(job);
            }
        }

        // compute input symbol <-> column mappings
        ImmutableMap.Builder<Symbol, ColumnHandle> mappings = ImmutableMap.builder();

        Iterator<ColumnHandle> columnIterator = columnHandles.iterator();
        Iterator<Field> fields = inputDescriptor.getFields().iterator();
        while (columnIterator.hasNext() && fields.hasNext()) {
            Symbol symbol = plan.getSymbol(fields.next());
            ColumnHandle column = columnIterator.next();
            mappings.put(symbol, column);
        }

        // create writer node
        Symbol output = symbolAllocator.newSymbol("imported_rows", Type.LONG);

        TableWriterNode writerNode = new TableWriterNode(idAllocator.getNextId(),
                plan.getRoot(),
                targetTable,
                mappings.build(),
                output);

        ImmutableMap<Field, Symbol> outputMappings = ImmutableMap.of(Iterables.getOnlyElement(analysis.getOutputDescriptor().getFields()), output);

        return new RelationPlan(outputMappings, writerNode);
    }


    private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis)
    {
        int i = 0;
        List<String> names = new ArrayList<>();
        ImmutableMap.Builder<String, Symbol> assignments = ImmutableMap.builder();
        for (Field field : analysis.getOutputDescriptor().getFields()) {
            String name = field.getName().orNull();
            while (name == null || names.contains(name)) {
                // TODO: this shouldn't be necessary once OutputNode uses Multimaps (requires updating to Jackson 2 for serialization support)
                i++;
                name = "_col" + i;
            }
            names.add(name);
            assignments.put(name, plan.getSymbol(field));
        }

        return new OutputNode(idAllocator.getNextId(), plan.getRoot(), names, assignments.build());
    }
}
