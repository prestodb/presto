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
package io.prestosql.connector.informationschema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;
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
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

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
        @Param("200")
        private String schemasCount = "200";
        @Param("200")
        private String tablesCount = "200";
        @Param("100")
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
            queryRunner.installPlugin(new Plugin()
            {
                @Override
                public Iterable<ConnectorFactory> getConnectorFactories()
                {
                    Function<ConnectorSession, List<String>> listSchemaNames = session -> IntStream.range(0, Integer.parseInt(schemasCount))
                            .boxed()
                            .map(i -> "stream_" + i)
                            .collect(toImmutableList());

                    BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables = (session, schemaNameOrNull) -> {
                        List<String> tables = IntStream.range(0, Integer.parseInt(tablesCount))
                                .boxed()
                                .map(i -> "table_" + i)
                                .collect(toImmutableList());
                        List<String> schemas;
                        if (schemaNameOrNull == null) {
                            schemas = listSchemaNames.apply(session);
                        }
                        else {
                            schemas = ImmutableList.of(schemaNameOrNull);
                        }
                        return schemas.stream()
                                .flatMap(schema -> tables.stream().map(table -> new SchemaTableName(schema, table)))
                                .collect(toImmutableList());
                    };

                    BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, TpchColumnHandle>> getColumnHandles = (session, tableHandle) -> IntStream.range(0, Integer.parseInt(columnsCount))
                            .boxed()
                            .map(i -> "column_" + i)
                            .collect(toImmutableMap(column -> column, column -> new TpchColumnHandle(column, createUnboundedVarcharType()) {}));
                    MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                            .withListSchemaNames(listSchemaNames)
                            .withListTables(listTables)
                            .withGetViews((session, prefix) -> ImmutableMap.of())
                            .withGetColumnHandles(getColumnHandles)
                            .build();
                    return ImmutableList.of(connectorFactory);
                }
            });
            queryRunner.createCatalog("test_catalog", "mock", ImmutableMap.of());

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
