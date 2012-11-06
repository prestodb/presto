package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TestingMetadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.serde.BlocksFileReader;
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

import java.io.IOException;
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
                                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "orderkey"),
                                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "custkey"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "orderstatus"),
                                new ColumnMetadata(TupleInfo.Type.DOUBLE, "totalprice"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "orderdate"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "orderpriority"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "clerk"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "shippriority"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "comment"))),
                new TableMetadata(SessionMetadata.DEFAULT_CATALOG, SessionMetadata.DEFAULT_SCHEMA, "LINEITEM",
                        ImmutableList.of(
                                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "orderkey"),
                                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "partkey"),
                                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "suppkey"),
                                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "linenumber"),
                                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "quantity"),
                                new ColumnMetadata(TupleInfo.Type.DOUBLE, "extendedprice"),
                                new ColumnMetadata(TupleInfo.Type.DOUBLE, "discount"),
                                new ColumnMetadata(TupleInfo.Type.DOUBLE, "tax"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "returnflag"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "linestatus"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "shipdate"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "commitdate"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "receiptdate"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "shipinstruct"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "shipmode"),
                                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "comment"))));

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
        StorageManager storage = new StorageManager()
        {
            @Override
            public long importTableShard(Operator source, String databaseName, String tableName)
                    throws IOException
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

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
