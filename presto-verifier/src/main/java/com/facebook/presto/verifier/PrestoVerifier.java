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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventClient;
import org.skife.jdbi.v2.DBI;

import java.util.List;
import java.util.Set;

public class PrestoVerifier
{
    protected PrestoVerifier()
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        new PrestoVerifier().run(args);
    }

    public void run(String[] args)
            throws Exception
    {
        if (args.length > 0) {
            System.setProperty("config", args[0]);
        }

        ImmutableList.Builder<Module> builder = ImmutableList.<Module>builder()
                .add(new PrestoVerifierModule())
                .addAll(getAdditionalModules());

        Bootstrap app = new Bootstrap(builder.build());
        Injector injector = app.strictConfig().initialize();

        VerifierConfig config = injector.getInstance(VerifierConfig.class);
        injector.injectMembers(this);
        EventClient eventClient = Iterables.getOnlyElement(injector.getInstance(Key.get(new TypeLiteral<Set<EventClient>>() {})));

        VerifierDao dao = new DBI(config.getQueryDatabase()).onDemand(VerifierDao.class);
        List<QueryPair> queries = dao.getQueriesBySuite(config.getSuite(), config.getMaxQueries());

        queries = applyOverrides(config, queries);
        queries = filterQueries(queries);

        // TODO: construct this with Guice
        Verifier verifier = new Verifier(System.out, config, eventClient);
        verifier.run(queries);

        injector.getInstance(LifeCycleManager.class).stop();
    }

    /**
     * Override this method to apply additional filtering to queries, before they're run.
     */
    protected List<QueryPair> filterQueries(List<QueryPair> queries)
    {
        return queries;
    }

    protected Iterable<Module> getAdditionalModules()
    {
        return ImmutableList.of();
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
}
