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
package io.prestosql.verifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventClient;
import io.airlift.log.Logger;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.sql.tree.AddColumn;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.Delete;
import io.prestosql.sql.tree.DropColumn;
import io.prestosql.sql.tree.DropTable;
import io.prestosql.sql.tree.DropView;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.Insert;
import io.prestosql.sql.tree.RenameColumn;
import io.prestosql.sql.tree.RenameTable;
import io.prestosql.sql.tree.ShowCatalogs;
import io.prestosql.sql.tree.ShowColumns;
import io.prestosql.sql.tree.ShowFunctions;
import io.prestosql.sql.tree.ShowSchemas;
import io.prestosql.sql.tree.ShowSession;
import io.prestosql.sql.tree.ShowTables;
import io.prestosql.sql.tree.Statement;
import io.prestosql.verifier.QueryRewriter.QueryRewriteException;
import org.jdbi.v3.core.ConnectionFactory;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static io.prestosql.verifier.QueryType.CREATE;
import static io.prestosql.verifier.QueryType.MODIFY;
import static io.prestosql.verifier.QueryType.READ;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;

@Command(name = "verify", description = "verify")
public class VerifyCommand
        implements Runnable
{
    private static final Logger LOG = Logger.get(VerifyCommand.class);

    @Arguments(description = "Config filename")
    public String configFilename;

    public void run()
    {
        if (configFilename != null) {
            // Read
            System.setProperty("config", configFilename);
        }

        ImmutableList.Builder<Module> builder = ImmutableList.<Module>builder()
                .add(new PrestoVerifierModule())
                .addAll(getAdditionalModules());

        Bootstrap app = new Bootstrap(builder.build());
        Injector injector;
        try {
            injector = app.strictConfig().initialize();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }

        try {
            VerifierConfig config = injector.getInstance(VerifierConfig.class);
            injector.injectMembers(this);
            Set<String> supportedEventClients = injector.getInstance(Key.get(new TypeLiteral<Set<String>>() {}, SupportedEventClients.class));
            for (String clientType : config.getEventClients()) {
                checkArgument(supportedEventClients.contains(clientType), "Unsupported event client: %s", clientType);
            }
            Set<EventClient> eventClients = injector.getInstance(Key.get(new TypeLiteral<Set<EventClient>>() {}));

            VerifierDao dao = Jdbi.create(getQueryDatabase(injector))
                    .installPlugin(new SqlObjectPlugin())
                    .onDemand(VerifierDao.class);

            ImmutableList.Builder<QueryPair> queriesBuilder = ImmutableList.builder();
            for (String suite : config.getSuites()) {
                queriesBuilder.addAll(dao.getQueriesBySuite(suite, config.getMaxQueries()));
            }

            List<QueryPair> queries = queriesBuilder.build();
            queries = applyOverrides(config, queries);
            queries = filterQueryTypes(new SqlParser(getParserOptions()), config, queries);
            queries = filterQueries(queries);
            if (config.getShadowWrites()) {
                Sets.SetView<QueryType> allowedTypes = Sets.union(config.getTestQueryTypes(), config.getControlQueryTypes());
                checkArgument(!Sets.intersection(allowedTypes, ImmutableSet.of(CREATE, MODIFY)).isEmpty(), "CREATE or MODIFY queries must be allowed in test or control to use write shadowing");
                queries = rewriteQueries(new SqlParser(getParserOptions()), config, queries);
            }

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
            int numFailedQueries = new Verifier(System.out, config, eventClients).run(queries);
            System.exit((numFailedQueries > 0) ? 1 : 0);
        }
        catch (InterruptedException | MalformedURLException e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally {
            try {
                injector.getInstance(LifeCycleManager.class).stop();
            }
            catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
    }

    private static void loadJdbcDriver(URL[] urls, String jdbcClassName)
    {
        try (URLClassLoader classLoader = new URLClassLoader(urls)) {
            Driver driver = (Driver) Class.forName(jdbcClassName, true, classLoader).getConstructor().newInstance();
            // The code calling the DriverManager to load the driver needs to be in the same class loader as the driver
            // In order to bypass this we create a shim that wraps the specified jdbc driver class.
            // TODO: Change the implementation to be DataSource based instead of DriverManager based.
            DriverManager.registerDriver(new ForwardingDriver(driver));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (SQLException | ReflectiveOperationException e) {
            throw new RuntimeException(e);
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
        File[] files = driverPath.listFiles((dir, name) -> {
            return name.endsWith(".jar");
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
     * Override this method to use a different method of acquiring a database connection.
     */
    protected ConnectionFactory getQueryDatabase(Injector injector)
    {
        VerifierConfig config = injector.getInstance(VerifierConfig.class);
        return () -> DriverManager.getConnection(config.getQueryDatabase());
    }

    /**
     * Override this method to change the parser options used when parsing queries to decide if they match the allowed query types
     */
    protected SqlParserOptions getParserOptions()
    {
        return new SqlParserOptions();
    }

    /**
     * Override this method to apply additional filtering to queries, before they're run.
     */
    protected List<QueryPair> filterQueries(List<QueryPair> queries)
    {
        return queries;
    }

    @VisibleForTesting
    static List<QueryPair> rewriteQueries(SqlParser parser, VerifierConfig config, List<QueryPair> queries)
    {
        QueryRewriter testRewriter = new QueryRewriter(
                parser,
                config.getTestGateway(),
                config.getShadowTestTablePrefix(),
                Optional.ofNullable(config.getTestCatalogOverride()),
                Optional.ofNullable(config.getTestSchemaOverride()),
                Optional.ofNullable(config.getTestUsernameOverride()),
                Optional.ofNullable(config.getTestPasswordOverride()),
                config.getDoublePrecision(),
                config.getTestTimeout());

        QueryRewriter controlRewriter = new QueryRewriter(
                parser,
                config.getControlGateway(),
                config.getShadowControlTablePrefix(),
                Optional.ofNullable(config.getControlCatalogOverride()),
                Optional.ofNullable(config.getControlSchemaOverride()),
                Optional.ofNullable(config.getControlUsernameOverride()),
                Optional.ofNullable(config.getControlPasswordOverride()),
                config.getDoublePrecision(),
                config.getControlTimeout());

        LOG.info("Rewriting %s queries using %s threads", queries.size(), config.getThreadCount());
        ExecutorService executor = newFixedThreadPool(config.getThreadCount());
        CompletionService<Optional<QueryPair>> completionService = new ExecutorCompletionService<>(executor);

        List<QueryPair> rewritten = new ArrayList<>();
        for (QueryPair pair : queries) {
            completionService.submit(() -> {
                try {
                    return Optional.of(new QueryPair(
                            pair.getSuite(),
                            pair.getName(),
                            testRewriter.shadowQuery(pair.getTest()),
                            controlRewriter.shadowQuery(pair.getControl())));
                }
                catch (QueryRewriteException | SQLException e) {
                    if (!config.isQuiet()) {
                        LOG.warn(e, "Failed to rewrite %s for shadowing. Skipping.", pair.getName());
                    }
                    return Optional.empty();
                }
            });
        }

        executor.shutdown();

        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            for (int n = 1; n <= queries.size(); n++) {
                completionService.take().get().ifPresent(rewritten::add);

                if (!config.isQuiet() && (stopwatch.elapsed(MINUTES) > 0)) {
                    stopwatch.reset().start();
                    LOG.info("Rewrite progress: %s valid, %s skipped, %.2f%% done",
                            rewritten.size(),
                            n - rewritten.size(),
                            (((double) n) / queries.size()) * 100);
                }
            }
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Query rewriting failed", e);
        }

        LOG.info("Rewrote %s queries into %s queries", queries.size(), rewritten.size());
        return rewritten;
    }

    private static List<QueryPair> filterQueryTypes(SqlParser parser, VerifierConfig config, List<QueryPair> queries)
    {
        ImmutableList.Builder<QueryPair> builder = ImmutableList.builder();
        for (QueryPair pair : queries) {
            if (queryTypeAllowed(parser, config.getControlQueryTypes(), pair.getControl()) && queryTypeAllowed(parser, config.getTestQueryTypes(), pair.getTest())) {
                builder.add(pair);
            }
        }
        return builder.build();
    }

    private static boolean queryTypeAllowed(SqlParser parser, Set<QueryType> allowedTypes, Query query)
    {
        Set<QueryType> types = EnumSet.noneOf(QueryType.class);
        try {
            for (String sql : query.getPreQueries()) {
                types.add(statementToQueryType(parser, sql));
            }
            types.add(statementToQueryType(parser, query.getQuery()));
            for (String sql : query.getPostQueries()) {
                types.add(statementToQueryType(parser, sql));
            }
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
        return allowedTypes.containsAll(types);
    }

    static QueryType statementToQueryType(SqlParser parser, String sql)
    {
        try {
            return statementToQueryType(parser.createStatement(sql, new ParsingOptions(AS_DOUBLE /* anything */)));
        }
        catch (RuntimeException e) {
            throw new UnsupportedOperationException();
        }
    }

    private static QueryType statementToQueryType(Statement statement)
    {
        if (statement instanceof AddColumn) {
            return MODIFY;
        }
        if (statement instanceof CreateTable) {
            return CREATE;
        }
        if (statement instanceof CreateTableAsSelect) {
            return CREATE;
        }
        if (statement instanceof CreateView) {
            if (((CreateView) statement).isReplace()) {
                return MODIFY;
            }
            return CREATE;
        }
        if (statement instanceof Delete) {
            return MODIFY;
        }
        if (statement instanceof DropTable) {
            return MODIFY;
        }
        if (statement instanceof DropView) {
            return MODIFY;
        }
        if (statement instanceof Explain) {
            if (((Explain) statement).isAnalyze()) {
                return statementToQueryType(((Explain) statement).getStatement());
            }
            return READ;
        }
        if (statement instanceof Insert) {
            return MODIFY;
        }
        if (statement instanceof io.prestosql.sql.tree.Query) {
            return READ;
        }
        if (statement instanceof RenameColumn) {
            return MODIFY;
        }
        if (statement instanceof DropColumn) {
            return MODIFY;
        }
        if (statement instanceof RenameTable) {
            return MODIFY;
        }
        if (statement instanceof ShowCatalogs) {
            return READ;
        }
        if (statement instanceof ShowColumns) {
            return READ;
        }
        if (statement instanceof ShowFunctions) {
            return READ;
        }
        if (statement instanceof ShowSchemas) {
            return READ;
        }
        if (statement instanceof ShowSession) {
            return READ;
        }
        if (statement instanceof ShowTables) {
            return READ;
        }
        throw new UnsupportedOperationException();
    }

    protected Iterable<Module> getAdditionalModules()
    {
        return ImmutableList.of();
    }

    private static List<QueryPair> applyOverrides(VerifierConfig config, List<QueryPair> queries)
    {
        return queries.stream()
                .map(input -> {
                    Query test = new Query(
                            Optional.ofNullable(config.getTestCatalogOverride()).orElse(input.getTest().getCatalog()),
                            Optional.ofNullable(config.getTestSchemaOverride()).orElse(input.getTest().getSchema()),
                            input.getTest().getPreQueries(),
                            input.getTest().getQuery(),
                            input.getTest().getPostQueries(),
                            Optional.ofNullable(config.getTestUsernameOverride()).orElse(input.getTest().getUsername()),
                            Optional.ofNullable(config.getTestPasswordOverride()).orElse(
                                    Optional.ofNullable(input.getTest().getPassword()).orElse(null)),
                            input.getTest().getSessionProperties());
                    Query control = new Query(
                            Optional.ofNullable(config.getControlCatalogOverride()).orElse(input.getControl().getCatalog()),
                            Optional.ofNullable(config.getControlSchemaOverride()).orElse(input.getControl().getSchema()),
                            input.getControl().getPreQueries(),
                            input.getControl().getQuery(),
                            input.getControl().getPostQueries(),
                            Optional.ofNullable(config.getControlUsernameOverride()).orElse(input.getControl().getUsername()),
                            Optional.ofNullable(config.getControlPasswordOverride()).orElse(
                                    Optional.ofNullable(input.getControl().getPassword()).orElse(null)),
                            input.getControl().getSessionProperties());
                    return new QueryPair(input.getSuite(), input.getName(), test, control);
                })
                .collect(toImmutableList());
    }
}
