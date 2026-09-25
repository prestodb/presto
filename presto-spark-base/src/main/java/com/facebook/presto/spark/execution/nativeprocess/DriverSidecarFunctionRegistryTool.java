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
package com.facebook.presto.spark.execution.nativeprocess;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.builtin.tools.WorkerFunctionRegistryTool;
import com.facebook.presto.builtin.tools.WorkerFunctionUtil;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunction;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Presto-on-Spark driver-side equivalent of
 * {@code com.facebook.presto.builtin.tools.NativeSidecarFunctionRegistryTool}, but pointed at
 * a driver-local {@link MetadataSidecarProcess} instead of a NodeManager-discovered worker.
 *
 * <p>HTTP-fetches {@code GET /v1/functions} from the driver-local sidecar URL and converts each
 * entry into a {@code SqlInvokedFunction} via
 * {@link WorkerFunctionUtil#createSqlInvokedFunction} with catalog {@code "presto"} so the
 * resulting handles register as built-ins (Path B from the design doc).
 */
public class DriverSidecarFunctionRegistryTool
        implements WorkerFunctionRegistryTool
{
    private static final Logger log = Logger.get(DriverSidecarFunctionRegistryTool.class);
    private static final String FUNCTION_SIGNATURES_ENDPOINT = "/v1/functions";
    private static final String CATALOG = "presto";

    private final OkHttpClient httpClient;
    private final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> functionSignatureMapJsonCodec;
    private final MetadataSidecarProcessFactory sidecarProcessFactory;

    private volatile UdfFunctionSignatureMap cachedSignatureMap;

    @Inject
    public DriverSidecarFunctionRegistryTool(
            @ForMetadataSidecar OkHttpClient httpClient,
            JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> functionSignatureMapJsonCodec,
            MetadataSidecarProcessFactory sidecarProcessFactory)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.functionSignatureMapJsonCodec = requireNonNull(functionSignatureMapJsonCodec, "functionSignatureMapJsonCodec is null");
        this.sidecarProcessFactory = requireNonNull(sidecarProcessFactory, "sidecarProcessFactory is null");
    }

    @Override
    public List<? extends SqlFunction> getWorkerFunctions()
    {
        return getCachedSignatureMap().getUDFSignatureMap().entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(metaInfo -> WorkerFunctionUtil.createSqlInvokedFunction(entry.getKey(), metaInfo, CATALOG)))
                .collect(toImmutableList());
    }

    @Override
    public Set<String> getRpcFunctionNames()
    {
        return getCachedSignatureMap().getUDFSignatureMap().entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(JsonBasedUdfFunctionMetadata::getIsRpcFunction))
                .map(Map.Entry::getKey)
                .map(name -> name.toLowerCase(Locale.ENGLISH))
                .collect(toImmutableSet());
    }

    private synchronized UdfFunctionSignatureMap getCachedSignatureMap()
    {
        if (cachedSignatureMap == null) {
            cachedSignatureMap = fetchFromSidecar();
        }
        return cachedSignatureMap;
    }

    private UdfFunctionSignatureMap fetchFromSidecar()
    {
        URI sidecarUri = sidecarProcessFactory.getOrStart();
        try {
            URI endpoint = sidecarUri.resolve(FUNCTION_SIGNATURES_ENDPOINT);
            Request request = new Request.Builder().url(endpoint.toString()).get().build();
            log.info("Fetching native function metadata from %s", endpoint);

            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new PrestoException(
                            GENERIC_INTERNAL_ERROR,
                            String.format("Metadata sidecar returned HTTP %d for %s", response.code(), endpoint));
                }
                ResponseBody body = response.body();
                if (body == null) {
                    throw new PrestoException(
                            GENERIC_INTERNAL_ERROR,
                            String.format("Metadata sidecar returned an empty body for %s", endpoint));
                }
                String responseJson = body.string();
                Map<String, List<JsonBasedUdfFunctionMetadata>> map =
                        functionSignatureMapJsonCodec.fromJson(responseJson);
                log.info("Fetched %d native function names from metadata sidecar", map.size());
                return new UdfFunctionSignatureMap(ImmutableMap.copyOf(map));
            }
            catch (IOException e) {
                throw new PrestoException(
                        GENERIC_INTERNAL_ERROR,
                        String.format("Failed to fetch functions from metadata sidecar at %s", endpoint),
                        e);
            }
        }
        finally {
            // Sidecar is only needed to fetch the function metadata once; shut it down
            // deterministically here rather than relying on @PreDestroy, which is not
            // always invoked on the Spark driver during executor shutdown.
            sidecarProcessFactory.close();
        }
    }
}
