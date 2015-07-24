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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventClient;
import org.skife.jdbi.v2.DBI;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;

public class PrestoVerifier
{
    public static final String SUPPORTED_EVENT_CLIENTS = "SUPPORTED_EVENT_CLIENTS";

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

        try {
            VerifierConfig config = injector.getInstance(VerifierConfig.class);
            injector.injectMembers(this);
            Set<String> supportedEventClients = injector.getInstance(Key.get(new TypeLiteral<Set<String>>() {}, Names.named(SUPPORTED_EVENT_CLIENTS)));
            for (String clientType : config.getEventClients()) {
                checkArgument(supportedEventClients.contains(clientType), "Unsupported event client: %s", clientType);
            }
            Set<EventClient> eventClients = injector.getInstance(Key.get(new TypeLiteral<Set<EventClient>>() {}));

            VerifierDao dao = new DBI(config.getQueryDatabase()).onDemand(VerifierDao.class);

            ImmutableList.Builder<QueryPair> queriesBuilder = ImmutableList.builder();
            for (String suite : config.getSuites()) {
                queriesBuilder.addAll(dao.getQueriesBySuite(suite, config.getMaxQueries()));
            }

            List<QueryPair> queries = queriesBuilder.build();
            queries = applyOverrides(config, queries);
            queries = filterQueries(queries);

            // Load jdbc drivers if needed
            if (config.getAdditionalJdbcDriverPath() != null) {
                List<URL> urlList = getUrls(config.getAdditionalJdbcDriverPath());
                URL[] urls = new URL[urlList.size()];
                urlList.toArray(urls);
                if (config.getTestJdbcDriverName() != null) {
                    loadJdbcDriver(urls, config.getTestJdbcDriverName());
                }
                if (config.getControlJdbcDriverName() != null) {
                    loadJdbcDriver(urls, config.getControlJdbcDriverName());
                }
            }

            // TODO: construct this with Guice
            Verifier verifier = new Verifier(System.out, config, eventClients);
            verifier.run(queries);
        }
        finally {
            injector.getInstance(LifeCycleManager.class).stop();
        }
    }

    private static void loadJdbcDriver(URL[] urls, String jdbcClassName)
    {
        try {
            try (URLClassLoader classLoader = new URLClassLoader(urls)) {
                Driver driver = (Driver) Class.forName(jdbcClassName, true, classLoader).getConstructor().newInstance();
                // The code calling the DriverManager to load the driver needs to be in the same class loader as the driver
                // In order to bypass this we create a shim that wraps the specified jdbc driver class.
                // TODO: Change the implementation to be DataSource based instead of DriverManager based.
                DriverManager.registerDriver(new ForwardingDriver(driver));
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static List<URL> getUrls(String path)
            throws MalformedURLException
    {
        ImmutableList.Builder<URL> urlList = ImmutableList.builder();
        File driverPath = new File(path);
        if (!driverPath.isDirectory()) {
            urlList.add(Paths.get(path).toUri().toURL());
            return urlList.build();
        }
        File[] files = driverPath.listFiles(new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".jar");
            }
        });
        if (files == null) {
            return urlList.build();
        }
        for (File file : files) {
            // Does not handle nested directories
            if (file.isDirectory()) {
                continue;
            }
            urlList.add(Paths.get(file.getAbsolutePath()).toUri().toURL());
        }
        return urlList.build();
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
        return queries.stream()
                .map(input -> {
                    Query test = new Query(
                            Optional.ofNullable(config.getTestCatalogOverride()).orElse(input.getTest().getCatalog()),
                            Optional.ofNullable(config.getTestSchemaOverride()).orElse(input.getTest().getSchema()),
                            input.getTest().getQuery(),
                            Optional.ofNullable(config.getTestUsernameOverride()).orElse(input.getTest().getUsername()),
                            Optional.ofNullable(config.getTestPasswordOverride()).orElse(
                                    Optional.ofNullable(input.getTest().getPassword()).orElse(null)),
                            input.getTest().getSessionProperties());
                    Query control = new Query(
                            Optional.ofNullable(config.getControlCatalogOverride()).orElse(input.getControl().getCatalog()),
                            Optional.ofNullable(config.getControlSchemaOverride()).orElse(input.getControl().getSchema()),
                            input.getControl().getQuery(),
                            Optional.ofNullable(config.getControlUsernameOverride()).orElse(input.getControl().getUsername()),
                            Optional.ofNullable(config.getControlPasswordOverride()).orElse(
                                    Optional.ofNullable(input.getControl().getPassword()).orElse(null)),
                            input.getControl().getSessionProperties());
                    return new QueryPair(input.getSuite(), input.getName(), test, control);
                })
                .collect(toImmutableList());
    }
}
