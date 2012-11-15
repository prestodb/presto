package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.LegacyStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TestingMetadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.sql.compiler.AnalysisResult;
import com.facebook.presto.sql.compiler.Analyzer;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExecutionPlanner;
import com.facebook.presto.sql.planner.PlanNode;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.Planner;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.antlr.runtime.RecognitionException;
import org.intellij.lang.annotations.Language;

import java.util.List;

public abstract class AbstractSqlBenchmark
        extends AbstractOperatorBenchmark
{
    private final PlanNode plan;
    private final SessionMetadata sessionMetadata;

    protected AbstractSqlBenchmark(String benchmarkName, int warmupIterations, int measuredIterations, @Language("SQL") String query)
    {
        super(benchmarkName, warmupIterations, measuredIterations);

        Metadata metadata = new TestingMetadata();
        List<TableMetadata> tables = ImmutableList.of(
                new TableMetadata(SessionMetadata.DEFAULT_CATALOG, SessionMetadata.DEFAULT_SCHEMA, "ORDERS",
                        ImmutableList.of(
                                new ColumnMetadata("orderkey", TupleInfo.Type.FIXED_INT_64),
                                new ColumnMetadata("custkey", TupleInfo.Type.FIXED_INT_64),
                                new ColumnMetadata("orderstatus", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("totalprice", TupleInfo.Type.DOUBLE),
                                new ColumnMetadata("orderdate", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("orderpriority", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("clerk", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("shippriority", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("comment", TupleInfo.Type.VARIABLE_BINARY))),
                new TableMetadata(SessionMetadata.DEFAULT_CATALOG, SessionMetadata.DEFAULT_SCHEMA, "LINEITEM",
                        ImmutableList.of(
                                new ColumnMetadata("orderkey", TupleInfo.Type.FIXED_INT_64),
                                new ColumnMetadata("partkey", TupleInfo.Type.FIXED_INT_64),
                                new ColumnMetadata("suppkey", TupleInfo.Type.FIXED_INT_64),
                                new ColumnMetadata("linenumber", TupleInfo.Type.FIXED_INT_64),
                                new ColumnMetadata("quantity", TupleInfo.Type.FIXED_INT_64),
                                new ColumnMetadata("extendedprice", TupleInfo.Type.DOUBLE),
                                new ColumnMetadata("discount", TupleInfo.Type.DOUBLE),
                                new ColumnMetadata("tax", TupleInfo.Type.DOUBLE),
                                new ColumnMetadata("returnflag", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("linestatus", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("shipdate", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("commitdate", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("receiptdate", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("shipinstruct", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("shipmode", TupleInfo.Type.VARIABLE_BINARY),
                                new ColumnMetadata("comment", TupleInfo.Type.VARIABLE_BINARY))));

        for (TableMetadata table : tables) {
            metadata.createTable(table);
        }

        try {
            Statement statement = SqlParser.createStatement(query);

            sessionMetadata = new SessionMetadata(metadata);
            AnalysisResult analysis = new Analyzer(sessionMetadata).analyze(statement);
            plan = new Planner().plan((Query) statement, analysis);

            new PlanPrinter().print(plan);
        }
        catch (RecognitionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected Operator createBenchmarkedOperator(final TpchBlocksProvider provider)
    {
        LegacyStorageManager storage = new LegacyStorageManager()
        {
            @Override
            public BlockIterable getBlocks(String databaseName, String tableName, int fieldIndex)
            {
                Preconditions.checkArgument(SessionMetadata.DEFAULT_CATALOG.equals(databaseName), "Unknown database: %s", databaseName);

                TpchSchema.Column[] columns;
                switch (tableName) {
                    case "orders":
                        columns = TpchSchema.Orders.values();
                        break;
                    case "lineitem":
                        columns = TpchSchema.LineItem.values();
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown table: " + tableName);
                }

                for (TpchSchema.Column column : columns) {
                    if (column.getIndex() == fieldIndex) {
                        return provider.getBlocks(column, BlocksFileEncoding.RAW);
                    }
                }

                throw new IllegalArgumentException("Unknown field index: " + fieldIndex);
            }
        };

        ExecutionPlanner executionPlanner = new ExecutionPlanner(sessionMetadata, storage);
        return executionPlanner.plan(plan);
    }
}
