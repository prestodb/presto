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
package com.facebook.presto.benchmark.driver;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.QuerySubmission;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.client.StatementStats;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.JsonResponseHandler;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.benchmark.driver.BenchmarkQueryResult.failResult;
import static com.facebook.presto.benchmark.driver.BenchmarkQueryResult.passResult;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BenchmarkQueryRunner
        implements Closeable
{
    private final int warm;
    private final int runs;
    private final boolean debug;
    private final int maxFailures;

    private final HttpClient httpClient;
    private final List<URI> nodes;
    private final JsonCodec<QueryResults> queryResultsCodec;
    private final JsonCodec<QuerySubmission> querySubmissionCodec;

    private int failures;

    public BenchmarkQueryRunner(int warm, int runs, boolean debug, int maxFailures, URI serverUri, Optional<HostAndPort> socksProxy)
    {
        checkArgument(warm >= 0, "warm is negative");
        this.warm = warm;

        checkArgument(runs >= 1, "runs must be at least 1");
        this.runs = runs;

        checkArgument(maxFailures >= 0, "maxFailures must be at least 0");
        this.maxFailures = maxFailures;

        this.debug = debug;

        this.queryResultsCodec = jsonCodec(QueryResults.class);
        this.querySubmissionCodec = jsonCodec(QuerySubmission.class);

        requireNonNull(socksProxy, "socksProxy is null");
        HttpClientConfig httpClientConfig = new HttpClientConfig();
        if (socksProxy.isPresent()) {
            httpClientConfig.setSocksProxy(socksProxy.get());
        }

        this.httpClient = new JettyHttpClient(httpClientConfig.setConnectTimeout(new Duration(10, TimeUnit.SECONDS)));

        nodes = getAllNodes(requireNonNull(serverUri, "serverUri is null"));
    }

    @SuppressWarnings("AssignmentToForLoopParameter")
    public BenchmarkQueryResult execute(Suite suite, ClientSession session, BenchmarkQuery query)
    {
        failures = 0;
        for (int i = 0; i < warm; ) {
            try {
                execute(session, query.getName(), query.getSql());
                i++;
                failures = 0;
            }
            catch (BenchmarkDriverExecutionException e) {
                return failResult(suite, query, e.getCause().getMessage());
            }
            catch (Exception e) {
                handleFailure(e);
            }
        }

        double[] wallTimeNanos = new double[runs];
        double[] processCpuTimeNanos = new double[runs];
        double[] queryCpuTimeNanos = new double[runs];
        for (int i = 0; i < runs; ) {
            try {
                long startCpuTime = getTotalCpuTime();
                long startWallTime = System.nanoTime();

                StatementStats statementStats = execute(session, query.getName(), query.getSql());

                long endWallTime = System.nanoTime();
                long endCpuTime = getTotalCpuTime();

                wallTimeNanos[i] = endWallTime - startWallTime;
                processCpuTimeNanos[i] = endCpuTime - startCpuTime;
                queryCpuTimeNanos[i] = MILLISECONDS.toNanos(statementStats.getCpuTimeMillis());

                i++;
                failures = 0;
            }
            catch (BenchmarkDriverExecutionException e) {
                return failResult(suite, query, e.getCause().getMessage());
            }
            catch (Exception e) {
                handleFailure(e);
            }
        }

        return passResult(
                suite,
                query,
                new Stat(wallTimeNanos),
                new Stat(processCpuTimeNanos),
                new Stat(queryCpuTimeNanos));
    }

    public List<String> getSchemas(ClientSession session)
    {
        failures = 0;
        while (true) {
            // start query
            StatementClient client = new StatementClient(httpClient, queryResultsCodec, querySubmissionCodec, session, "show schemas");

            // read query output
            ImmutableList.Builder<String> schemas = ImmutableList.builder();
            while (client.isValid() && client.advance()) {
                // we do not process the output
                Iterable<List<Object>> data = client.current().getData();
                if (data != null) {
                    for (List<Object> objects : data) {
                        schemas.add(objects.get(0).toString());
                    }
                }
            }

            // verify final state
            if (client.isClosed()) {
                throw new IllegalStateException("Query aborted by user");
            }

            if (client.isGone()) {
                throw new IllegalStateException("Query is gone (server restarted?)");
            }

            QueryError resultsError = client.finalResults().getError();
            if (resultsError != null) {
                RuntimeException cause = null;
                if (resultsError.getFailureInfo() != null) {
                    cause = resultsError.getFailureInfo().toException();
                }
                handleFailure(cause);

                continue;
            }

            return schemas.build();
        }
    }

    private StatementStats execute(ClientSession session, String name, String query)
    {
        // start query
        StatementClient client = new StatementClient(httpClient, queryResultsCodec, querySubmissionCodec, session, query);

        // read query output
        while (client.isValid() && client.advance()) {
            // we do not process the output
        }

        // verify final state
        if (client.isClosed()) {
            throw new IllegalStateException("Query aborted by user");
        }

        if (client.isGone()) {
            throw new IllegalStateException("Query is gone (server restarted?)");
        }

        QueryError resultsError = client.finalResults().getError();
        if (resultsError != null) {
            RuntimeException cause = null;
            if (resultsError.getFailureInfo() != null) {
                cause = resultsError.getFailureInfo().toException();
            }

            throw new BenchmarkDriverExecutionException(format("Query %s failed: %s", name, resultsError.getMessage()), cause);
        }

        return client.finalResults().getStats();
    }

    @Override
    public void close()
    {
        httpClient.close();
    }

    @SuppressWarnings("CallToPrintStackTrace")
    public void handleFailure(Exception e)
    {
        if (debug) {
            if (e == null) {
                e = new RuntimeException("Unknown error");
            }
            e.printStackTrace();
        }

        failures++;

        if (failures > maxFailures) {
            throw new RuntimeException("To many consecutive failures");
        }

        try {
            TimeUnit.SECONDS.sleep(5);
        }
        catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(interruptedException);
        }
    }

    private long getTotalCpuTime()
    {
        long totalCpuTime = 0;
        for (URI server : nodes) {
            URI addressUri = uriBuilderFrom(server).replacePath("/v1/jmx/mbean/java.lang:type=OperatingSystem/ProcessCpuTime").build();
            String data = httpClient.execute(prepareGet().setUri(addressUri).build(), createStringResponseHandler()).getBody();
            totalCpuTime += parseLong(data.trim());
        }
        return TimeUnit.NANOSECONDS.toNanos(totalCpuTime);
    }

    private List<URI> getAllNodes(URI server)
    {
        Request request = prepareGet().setUri(uriBuilderFrom(server).replacePath("/v1/service/presto").build()).build();
        JsonResponseHandler<ServiceDescriptorsRepresentation> responseHandler = createJsonResponseHandler(jsonCodec(ServiceDescriptorsRepresentation.class));
        ServiceDescriptorsRepresentation serviceDescriptors = httpClient.execute(request, responseHandler);

        ImmutableList.Builder<URI> addresses = ImmutableList.builder();
        for (ServiceDescriptor serviceDescriptor : serviceDescriptors.getServiceDescriptors()) {
            String httpUri = serviceDescriptor.getProperties().get("http");
            if (httpUri != null) {
                addresses.add(URI.create(httpUri));
            }
        }
        return addresses.build();
    }
}
