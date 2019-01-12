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
package io.prestosql.benchmark.driver;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.prestosql.client.ClientSession;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BenchmarkDriver
        implements Closeable
{
    private final ClientSession clientSession;
    private final List<BenchmarkQuery> queries;
    private final BenchmarkResultsStore resultsStore;
    private final BenchmarkQueryRunner queryRunner;

    public BenchmarkDriver(BenchmarkResultsStore resultsStore,
            ClientSession clientSession,
            Iterable<BenchmarkQuery> queries,
            int warm,
            int runs,
            boolean debug,
            int maxFailures,
            Optional<HostAndPort> socksProxy)
    {
        this.resultsStore = requireNonNull(resultsStore, "resultsStore is null");
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
        this.queries = ImmutableList.copyOf(requireNonNull(queries, "queries is null"));

        queryRunner = new BenchmarkQueryRunner(warm, runs, debug, maxFailures, clientSession.getServer(), socksProxy);
    }

    public void run(Suite suite)
    {
        // select queries to run
        List<BenchmarkQuery> queries = suite.selectQueries(this.queries);
        if (queries.isEmpty()) {
            return;
        }

        Map<String, String> properties = new HashMap<>();
        properties.putAll(clientSession.getProperties());
        properties.putAll(suite.getSessionProperties());
        ClientSession session = ClientSession.builder(clientSession)
                .withProperties(properties)
                .build();

        // select schemas to use
        List<BenchmarkSchema> benchmarkSchemas;
        if (!suite.getSchemaNameTemplates().isEmpty()) {
            List<String> schemas = queryRunner.getSchemas(session);
            benchmarkSchemas = suite.selectSchemas(schemas);
        }
        else {
            benchmarkSchemas = ImmutableList.of(new BenchmarkSchema(session.getSchema()));
        }
        if (benchmarkSchemas.isEmpty()) {
            return;
        }

        for (BenchmarkSchema benchmarkSchema : benchmarkSchemas) {
            for (BenchmarkQuery benchmarkQuery : queries) {
                session = ClientSession.builder(session)
                        .withCatalog(session.getCatalog())
                        .withSchema(benchmarkSchema.getName())
                        .build();
                BenchmarkQueryResult result = queryRunner.execute(suite, session, benchmarkQuery);

                resultsStore.store(benchmarkSchema, result);
            }
        }
    }

    @Override
    public void close()
    {
        queryRunner.close();
    }
}
