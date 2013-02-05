package com.facebook.presto;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.server.HackPlanFragmentSourceProvider;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.AnalysisResult;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.TableScanPlanFragmentSource;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tpch.TpchSplit;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.intellij.lang.annotations.Language;

import static com.facebook.presto.util.MaterializedResult.materialize;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertTrue;

public class TestLocalQueries
    extends AbstractTestQueries
{
    private String catalog;
    private String schema;
    private DataStreamProvider dataStreamProvider;
    private Metadata metadata;

    @Override
    protected void setUpQueryFramework(String catalog, String schema, DataStreamProvider dataStreamProvider, Metadata metadata)
            throws Exception
    {
        this.catalog = catalog;
        this.schema = schema;
        this.dataStreamProvider = dataStreamProvider;
        this.metadata = metadata;
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        Statement statement = SqlParser.createStatement(sql);

        Session session = new Session(null, catalog, schema);

        Analyzer analyzer = new Analyzer(session, metadata);

        AnalysisResult analysis = analyzer.analyze(statement);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanNode plan = new LogicalPlanner(session, metadata, idAllocator).plan(analysis);
        new PlanPrinter().print(plan, analysis.getTypes());

        SubPlan subplan = new DistributedLogicalPlanner(metadata, idAllocator).createSubplans(plan, analysis.getSymbolAllocator(), true);
        assertTrue(subplan.getChildren().isEmpty(), "Expected subplan to have no children");

        ImmutableMap.Builder<PlanNodeId, TableScanPlanFragmentSource> builder = ImmutableMap.builder();
        for (PlanNode source : subplan.getFragment().getSources()) {
            TableScanNode tableScan = (TableScanNode) source;
            TpchTableHandle handle = (TpchTableHandle) tableScan.getTable();

            builder.put(tableScan.getId(), new TableScanPlanFragmentSource(new TpchSplit(handle)));
        }

        DataSize maxOperatorMemoryUsage = new DataSize(50, MEGABYTE);
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(session, metadata,
                new HackPlanFragmentSourceProvider(dataStreamProvider, null, new QueryManagerConfig()),
                analysis.getTypes(),
                null,
                builder.build(),
                ImmutableMap.<PlanNodeId, ExchangePlanFragmentSource>of(),
                new OperatorStats(),
                new SourceHashProviderFactory(maxOperatorMemoryUsage),
                maxOperatorMemoryUsage
        );

        Operator operator = executionPlanner.plan(plan);

        return materialize(operator);
    }
}
