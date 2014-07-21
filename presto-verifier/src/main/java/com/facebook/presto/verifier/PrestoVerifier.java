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
package com.facebook.presto.verifier;

import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationLoader;
import io.airlift.event.client.EventClient;
import org.skife.jdbi.v2.DBI;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class PrestoVerifier
{
    private final VerifierConfig config;

    protected PrestoVerifier(VerifierConfig config)
    {
        this.config = checkNotNull(config, "config is null");
    }

    public static void main(String[] args)
            throws Exception
    {
        Optional<String> configPath = Optional.fromNullable((args.length > 0) ? args[0] : null);
        VerifierConfig config = loadConfig(VerifierConfig.class, configPath);
        PrestoVerifier verifier = new PrestoVerifier(config);
        verifier.run();

        // HttpClient's threads don't seem to shutdown properly, so force the VM to exit
        System.exit(0);
    }

    public void run()
            throws Exception
    {
        EventClient eventClient = getEventClient(config);

        VerifierDao dao = new DBI(config.getQueryDatabase()).onDemand(VerifierDao.class);
        List<QueryPair> queries = dao.getQueriesBySuite(config.getSuite(), config.getMaxQueries());

        queries = applyOverrides(config, queries);
        queries = filterQueries(queries);

        Verifier verifier = new Verifier(System.out, config, eventClient);
        verifier.run(queries);
    }

    /**
     * Override this method to apply additional filtering to queries, before they're run.
     */
    protected List<QueryPair> filterQueries(List<QueryPair> queries)
    {
        return queries;
    }

    private static List<QueryPair> applyOverrides(final VerifierConfig config, List<QueryPair> queries)
    {
        return IterableTransformer.on(queries).transform(new Function<QueryPair, QueryPair>()
        {
            @Override
            public QueryPair apply(QueryPair input)
            {
                Query test = new Query(
                        Optional.fromNullable(config.getTestCatalogOverride()).or(input.getTest().getCatalog()),
                        Optional.fromNullable(config.getTestSchemaOverride()).or(input.getTest().getSchema()),
                        input.getTest().getQuery());
                Query control = new Query(
                        Optional.fromNullable(config.getControlCatalogOverride()).or(input.getControl().getCatalog()),
                        Optional.fromNullable(config.getControlSchemaOverride()).or(input.getControl().getSchema()),
                        input.getControl().getQuery());
                return new QueryPair(input.getSuite(), input.getName(), test, control);
            }
        }).list();
    }

    private EventClient getEventClient(VerifierConfig config)
            throws FileNotFoundException
    {
        switch (config.getEventClient()) {
            case "human-readable":
                return new HumanReadableEventClient(System.out, config.isAlwaysReport());
            case "file":
                checkNotNull(config.getEventLogFile(), "event log file path is null");
                return new JsonEventClient(new PrintStream(config.getEventLogFile()));
        }
        EventClient client = getEventClient(config.getEventClient());
        if (client != null) {
            return client;
        }
        throw new RuntimeException(format("Unsupported event client %s", config.getEventClient()));
    }

    /**
     * Override this method to provide other event client implementations.
     */
    protected EventClient getEventClient(String name)
    {
        return null;
    }

    public static <T> T loadConfig(Class<T> clazz, Optional<String> path)
            throws IOException
    {
        ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
        ConfigurationLoader loader = new ConfigurationLoader();
        if (path.isPresent()) {
            map.putAll(loader.loadPropertiesFrom(path.get()));
        }
        map.putAll(loader.getSystemProperties());
        return new ConfigurationFactory(map.build()).build(clazz);
    }
}
