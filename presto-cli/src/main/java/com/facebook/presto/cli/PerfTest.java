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
package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.PrestoHeaders;
import com.facebook.presto.sql.parser.StatementSplitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.cli.ClientOptions.parseServer;
import static com.facebook.presto.sql.parser.StatementSplitter.Statement;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.airline.SingleCommand.singleCommand;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Command(name = "presto", description = "Presto interactive console")
public class PerfTest
{
    private static final String USER_AGENT_VALUE = PerfTest.class.getSimpleName() +
            "/" +
            firstNonNull(PerfTest.class.getPackage().getImplementationVersion(), "unknown");

    @Inject
    public HelpOption helpOption;

    @Option(name = "--server", title = "server", description = "Presto server location (default: localhost:8080)")
    public String server = "localhost:8080";

    @Option(name = "--catalog", title = "catalog", description = "Default catalog")
    public String catalog;

    @Option(name = "--schema", title = "schema", description = "Default schema")
    public String schema;

    @Option(name = {"-f", "--file"}, title = "file", description = "Execute statements from file and exit")
    public String file;

    @Option(name = "--debug", title = "debug", description = "Enable debug information")
    public boolean debug;

    @Option(name = {"-r", "--runs"}, title = "number", description = "Number of runs until exit (default: 10)")
    public int runs = 10;

    @Option(name = "--timeout", title = "timeout", description = "Timeout for HTTP-Client to wait for query results (default: 600)")
    public int timeout = 600;

    @Option(name = "--client-request-timeout", title = "client request timeout", description = "Client request timeout (default: 2m)")
    public Duration clientRequestTimeout = new Duration(2, TimeUnit.MINUTES);

    public void run()
            throws Exception
    {
        initializeLogging(debug);
        List<String> queries = loadQueries();

        try (ParallelQueryRunner parallelQueryRunner = new ParallelQueryRunner(16, parseServer(server), catalog, schema, debug, timeout, clientRequestTimeout)) {
            for (int loop = 0; loop < runs; loop++) {
                executeQueries(queries, parallelQueryRunner, 1);
                executeQueries(queries, parallelQueryRunner, 2);
                executeQueries(queries, parallelQueryRunner, 4);
                executeQueries(queries, parallelQueryRunner, 8);
                executeQueries(queries, parallelQueryRunner, 16);
            }
        }
    }

    private static void executeQueries(List<String> queries, ParallelQueryRunner parallelQueryRunner, int parallelism)
            throws Exception
    {
        Duration duration = parallelQueryRunner.executeCommands(parallelism, queries);
        System.out.printf("%2d: %s\n", parallelism, duration.convertTo(TimeUnit.SECONDS));
    }

    private List<String> loadQueries()
    {
        try {
            String query = Files.toString(new File(file), UTF_8);
            StatementSplitter splitter = new StatementSplitter(query + ";");
            return ImmutableList.copyOf(transform(splitter.getCompleteStatements(), Statement::statement));
        }
        catch (IOException e) {
            throw new RuntimeException(format("Error reading from file %s: %s", file, e.getMessage()));
        }
    }

    public static class ParallelQueryRunner
            implements Closeable
    {
        private final ListeningExecutorService executor;
        private final List<QueryRunner> runners;

        public ParallelQueryRunner(int maxParallelism, URI server, String catalog, String schema, boolean debug, int timeout, Duration clientRequestTimeout)
        {
            executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("query-runner-%s")));

            ImmutableList.Builder<QueryRunner> runners = ImmutableList.builder();
            for (int i = 0; i < maxParallelism; i++) {
                ClientSession session = new ClientSession(
                        server,
                        "test-" + i,
                        "presto-perf",
                        null,
                        catalog,
                        schema,
                        TimeZone.getDefault().getID(),
                        Locale.getDefault(),
                        ImmutableMap.of(),
                        null,
                        debug,
                        clientRequestTimeout);
                runners.add(new QueryRunner(session, executor, timeout));
            }
            this.runners = runners.build();
        }

        public Duration executeCommands(int parallelism, List<String> queries)
                throws Exception
        {
            checkArgument(parallelism >= 0, "parallelism is negative");
            checkArgument(parallelism <= runners.size(), "parallelism is greater than maxParallelism");
            requireNonNull(queries, "queries is null");

            CountDownLatch remainingQueries = new CountDownLatch(queries.size());
            BlockingQueue<String> queue = new ArrayBlockingQueue<>(queries.size(), false, queries);

            List<ListenableFuture<?>> futures = new ArrayList<>(parallelism);
            long start = System.nanoTime();
            for (int i = 0; i < parallelism; i++) {
                QueryRunner runner = runners.get(i);
                futures.add(runner.execute(queue, remainingQueries));
            }

            // kill test if anything fails
            ListenableFuture<List<Object>> allFutures = Futures.allAsList(futures);
            Futures.addCallback(allFutures, new FutureCallback<List<Object>>()
            {
                @Override
                public void onSuccess(@Nullable List<Object> result)
                {
                }

                @Override
                public void onFailure(Throwable t)
                {
                    System.err.println("Run failed");
                    t.printStackTrace(System.err);
                    System.exit(1);
                }
            }, executor);

            remainingQueries.await();
            Duration executionTime = Duration.nanosSince(start);

            // wait for runners to spin-down
            allFutures.get();

            return executionTime;
        }

        @Override
        public void close()
                throws IOException
        {
            for (QueryRunner runner : runners) {
                try {
                    runner.close();
                }
                catch (Exception ignored) {
                }
            }
        }
    }

    public static class QueryRunner
            implements Closeable
    {
        private final ClientSession session;
        private final ListeningExecutorService executor;
        private final HttpClient httpClient;

        public QueryRunner(ClientSession session, ListeningExecutorService executor, int timeout)
        {
            this.session = session;
            this.executor = executor;

            HttpClientConfig clientConfig = new HttpClientConfig();
            clientConfig.setConnectTimeout(new Duration(10, TimeUnit.SECONDS));
            clientConfig.setIdleTimeout(new Duration(timeout, TimeUnit.SECONDS));
            httpClient = new JettyHttpClient(clientConfig);
        }

        public ListenableFuture<?> execute(BlockingQueue<String> queue, CountDownLatch remainingQueries)
        {
            return executor.submit(() -> {
                for (String query = queue.poll(); query != null; query = queue.poll()) {
                    execute(query);
                    remainingQueries.countDown();
                }
            });
        }

        public void execute(String query)
        {
            Request request = buildQueryRequest(session, query);
            StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
            if (response.getStatusCode() != 200) {
                throw new RuntimeException("Query failed: [" + response.getStatusCode() + "] " + response.getStatusMessage());
            }
        }

        private static Request buildQueryRequest(ClientSession session, String query)
        {
            Request.Builder builder = preparePost()
                    .setUri(uriBuilderFrom(session.getServer()).replacePath("/v1/execute").build())
                    .setBodyGenerator(createStaticBodyGenerator(query, UTF_8));

            if (session.getUser() != null) {
                builder.setHeader(PrestoHeaders.PRESTO_USER, session.getUser());
            }
            if (session.getSource() != null) {
                builder.setHeader(PrestoHeaders.PRESTO_SOURCE, session.getSource());
            }
            if (session.getCatalog() != null) {
                builder.setHeader(PrestoHeaders.PRESTO_CATALOG, session.getCatalog());
            }
            if (session.getSchema() != null) {
                builder.setHeader(PrestoHeaders.PRESTO_SCHEMA, session.getSchema());
            }
            builder.setHeader(PrestoHeaders.PRESTO_TIME_ZONE, session.getTimeZoneId());
            builder.setHeader(USER_AGENT, USER_AGENT_VALUE);

            return builder.build();
        }

        @Override
        public void close()
        {
            httpClient.close();
        }
    }

    private static void initializeLogging(boolean debug)
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;
        try {
            if (debug) {
                Logging logging = Logging.initialize();
                logging.configure(new LoggingConfiguration());
                logging.setLevel("com.facebook.presto", Level.DEBUG);
            }
            else {
                System.setOut(new PrintStream(nullOutputStream()));
                System.setErr(new PrintStream(nullOutputStream()));

                Logging logging = Logging.initialize();
                logging.configure(new LoggingConfiguration());
                logging.disableConsole();
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        PerfTest perfTest = singleCommand(PerfTest.class).parse(args);

        if (perfTest.helpOption.showHelpIfRequested()) {
            return;
        }

        perfTest.run();
    }
}
