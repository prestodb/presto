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
package com.facebook.presto.connector.informationschema;

import com.facebook.presto.Session;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchHandleResolver;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.facebook.presto.tpch.TpchSplitManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 4)
@Fork(1)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkInformationSchema
{
    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        private final Map<String, String> queries = ImmutableMap.of(
                "FULL_SCAN", "SELECT count(*) FROM information_schema.columns",
                "LIKE_PREDICATE", "SELECT count(*) FROM information_schema.columns WHERE table_name LIKE 'table_0' AND table_schema LIKE 'schema_0'",
                "MIXED_PREDICATE", "SELECT count(*) FROM information_schema.columns WHERE table_name LIKE 'table_0' AND table_schema = 'schema_0'");

        @Param({"FULL_SCAN", "LIKE_PREDICATE", "MIXED_PREDICATE"})
        private String queryId = "LIKE_PREDICATE";
        @Param({"200"})
        private String schemasCount = "200";
        @Param({"200"})
        private String tablesCount = "200";
        @Param({"100"})
        private String columnsCount = "100";

        private QueryRunner queryRunner;

        private Session session = testSessionBuilder()
                    .setCatalog("test_catalog")
                    .setSchema("test_schema")
                    .build();

        private String query;

        @Setup
        public void setup()
                throws Exception
        {
            queryRunner = DistributedQueryRunner.builder(session).build();
            queryRunner.installPlugin(new Plugin() {
                @Override
                public Iterable<ConnectorFactory> getConnectorFactories()
                {
                    return ImmutableList.of(new TestingInformationSchemaConnectorFactory(schemasCount, tablesCount, columnsCount));
                }
            });
            queryRunner.createCatalog("test_catalog", "testing_information_schema", ImmutableMap.of());

            query = queries.get(queryId);
        }

        @TearDown
        public void tearDown()
        {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Benchmark
    public MaterializedResult queryInformationSchema(BenchmarkData benchmarkData)
    {
        return benchmarkData.queryRunner.execute(benchmarkData.query);
    }

    private static class TestingInformationSchemaConnectorFactory
            implements ConnectorFactory
    {
        private final int schemasCount;
        private final int tablesCount;
        private final int columnsCount;

        public TestingInformationSchemaConnectorFactory(String schemasCount, String tablesCount, String columnsCount)
        {
            this.schemasCount = Integer.parseInt(schemasCount);
            this.tablesCount = Integer.parseInt(tablesCount);
            this.columnsCount = Integer.parseInt(columnsCount);
        }

        @Override
        public String getName()
        {
            return "testing_information_schema";
        }

        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return new TpchHandleResolver();
        }

        @Override
        public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
        {
            return new Connector()
            {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
                {
                    return new ConnectorTransactionHandle() {};
                }

                @Override
                public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
                {
                    return new ConnectorMetadata()
                    {
                        @Override
                        public List<String> listSchemaNames(ConnectorSession session)
                        {
                            return IntStream.range(0, schemasCount)
                                    .boxed()
                                    .map(i -> "stream_" + i)
                                    .collect(toImmutableList());
                        }

                        @Override
                        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
                        {
                            return new ConnectorTableHandle() {};
                        }

                        @Override
                        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
                        {
                            return null;
                        }

                        @Override
                        public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
                        {
                            List<String> tables = IntStream.range(0, tablesCount)
                                    .boxed()
                                    .map(i -> "table_" + i)
                                    .collect(toImmutableList());
                            List<String> schemas;
                            if (schemaNameOrNull == null) {
                                schemas = listSchemaNames(session);
                            }
                            else {
                                schemas = ImmutableList.of(schemaNameOrNull);
                            }
                            return schemas.stream()
                                    .flatMap(schema -> tables.stream().map(table -> new SchemaTableName(schema, table)))
                                    .collect(toImmutableList());
                        }

                        @Override
                        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
                        {
                            return IntStream.range(0, columnsCount)
                                    .boxed()
                                    .map(i -> "column_" + i)
                                    .collect(toImmutableMap(column -> column, column -> new TpchColumnHandle(column, createUnboundedVarcharType()) {}));
                        }

                        @Override
                        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
                        {
                            TpchColumnHandle tpchColumnHandle = (TpchColumnHandle) columnHandle;
                            return new ColumnMetadata(tpchColumnHandle.getColumnName(), tpchColumnHandle.getType());
                        }

                        @Override
                        public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
                        {
                            return listTables(session, prefix.getSchemaName()).stream()
                                    .collect(toImmutableMap(table -> table, table -> IntStream.range(0, 100)
                                            .boxed()
                                            .map(i -> new ColumnMetadata("column_" + i, createUnboundedVarcharType()))
                                            .collect(toImmutableList())));
                        }

                        @Override
                        public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
                        {
                            return ImmutableList.of();
                        }

                        @Override
                        public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
                        {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override
                public ConnectorSplitManager getSplitManager()
                {
                    return new TpchSplitManager(context.getNodeManager(), 1);
                }

                @Override
                public ConnectorRecordSetProvider getRecordSetProvider()
                {
                    return new TpchRecordSetProvider();
                }
            };
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        try {
            new BenchmarkInformationSchema().queryInformationSchema(data);
        }
        finally {
            data.tearDown();
        }

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkInformationSchema.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
