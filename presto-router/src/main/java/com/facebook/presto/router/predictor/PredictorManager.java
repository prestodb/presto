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
package com.facebook.presto.router.predictor;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.router.RouterConfig;
import com.facebook.presto.router.spec.RouterSpec;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The manager of fetching predicted resource usage of a SQL statement from the
 * Presto query predictor.
 * Note that it does not validate the SQL statements passed in.
 */
public class PredictorManager
{
    private static final Logger log = Logger.get(PredictorManager.class);
    private static final JsonCodec<RouterSpec> CODEC = new JsonCodecFactory(
            () -> new JsonObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(RouterSpec.class);

    private final RemoteQueryFactory remoteQueryFactory;
    private final URI uri;

    @Inject
    public PredictorManager(RemoteQueryFactory remoteQueryFactory, RouterConfig config)
    {
        RouterSpec routerSpec = parseRouterConfig(config)
                .orElseThrow(() -> new PrestoException(CONFIGURATION_INVALID, "Failed to load router config"));

        this.remoteQueryFactory = requireNonNull(remoteQueryFactory, "");
        this.uri = routerSpec.getPredictorUri().orElse(null);
    }

    public Optional<ResourceGroup> fetchPrediction(String statement)
    {
        try {
            return Optional.of(new ResourceGroup(fetchCpuPrediction(statement).orElse(null), fetchMemoryPrediction(statement).orElse(null)));
        }
        catch (Exception e) {
            log.error("Error in fetching prediction", e);
        }
        return Optional.empty();
    }

    public Optional<ResourceGroup> fetchPredictionParallel(String statement)
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        Future<CpuInfo> cpuInfoFuture = executor.submit(() -> fetchCpuPrediction(statement).orElse(null));
        Future<MemoryInfo> memoryInfoFuture = executor.submit(() -> fetchMemoryPrediction(statement).orElse(null));
        try {
            return Optional.of(new ResourceGroup(cpuInfoFuture.get(), memoryInfoFuture.get()));
        }
        catch (Exception e) {
            log.error("Error in fetching prediction in parallel", e);
        }
        return Optional.empty();
    }

    public Optional<CpuInfo> fetchCpuPrediction(String statement)
    {
        RemoteQueryCpu remoteQueryCpu;
        try {
            remoteQueryCpu = this.remoteQueryFactory.createRemoteQueryCPU(this.uri);
            remoteQueryCpu.execute(statement);
            return Optional.of(remoteQueryCpu.getCpuInfo());
        }
        catch (Exception e) {
            log.error("Error in fetching CPU prediction", e);
        }
        return Optional.empty();
    }

    public Optional<MemoryInfo> fetchMemoryPrediction(String statement)
    {
        RemoteQueryMemory remoteQueryMemory;
        try {
            remoteQueryMemory = this.remoteQueryFactory.createRemoteQueryMemory(this.uri);
            remoteQueryMemory.execute(statement);
            return Optional.of(remoteQueryMemory.getMemoryInfo());
        }
        catch (Exception e) {
            log.error("Error in fetching memory prediction", e);
        }
        return Optional.empty();
    }

    private static Optional<RouterSpec> parseRouterConfig(RouterConfig config)
    {
        Optional<RouterSpec> routerSpec;
        try {
            routerSpec = Optional.of(CODEC.fromJson(Files.readAllBytes(Paths.get(config.getConfigFile()))));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (IllegalArgumentException e) {
            handleConfigIllegalArgumentException(e);
            throw e;
        }

        return routerSpec;
    }

    private static void handleConfigIllegalArgumentException(IllegalArgumentException e)
    {
        Throwable cause = e.getCause();
        if (cause instanceof UnrecognizedPropertyException) {
            UnrecognizedPropertyException ex = (UnrecognizedPropertyException) cause;
            String message = format("Unknown property at line %s:%s: %s",
                    ex.getLocation().getLineNr(),
                    ex.getLocation().getColumnNr(),
                    ex.getPropertyName());
            throw new IllegalArgumentException(message, e);
        }
        if (cause instanceof JsonMappingException) {
            // remove the extra "through reference chain" message
            if (cause.getCause() != null) {
                cause = cause.getCause();
            }
            throw new IllegalArgumentException(cause.getMessage(), e);
        }
    }
}
