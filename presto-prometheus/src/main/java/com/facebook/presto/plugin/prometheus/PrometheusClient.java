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
package com.facebook.presto.plugin.prometheus;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.prometheus.PrometheusColumn.mapType;
import static com.facebook.presto.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_TABLES_METRICS_RETRIEVE_ERROR;
import static com.facebook.presto.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_UNKNOWN_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PrometheusClient
{
    public static final String METRICS_ENDPOINT = "/api/v1/label/__name__/values";

    private static final OkHttpClient httpClient = new OkHttpClient.Builder().build();

    private final Optional<File> bearerTokenFile;
    private final Supplier<Map<String, Object>> tableSupplier;
    private final Type varcharMapType;

    private static final Logger log = Logger.get(PrometheusClient.class);

    @Inject
    public PrometheusClient(PrometheusConnectorConfig config, JsonCodec<Map<String, Object>> metricCodec, TypeManager typeManager)
    {
        requireNonNull(config, "config is null");
        requireNonNull(metricCodec, "metricCodec is null");
        requireNonNull(typeManager, "typeManager is null");

        bearerTokenFile = config.getBearerTokenFile();
        URI prometheusMetricsUri = getPrometheusMetricsURI(config.getPrometheusURI());
        tableSupplier = Suppliers.memoizeWithExpiration(
                () -> fetchMetrics(metricCodec, prometheusMetricsUri),
                config.getCacheDuration().toMillis(),
                MILLISECONDS);
        varcharMapType = typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
    }

    private static URI getPrometheusMetricsURI(URI prometheusUri)
    {
        try {
            // endpoint to retrieve metric names from Prometheus
            return new URI(prometheusUri.getScheme(), prometheusUri.getAuthority(), prometheusUri.getPath() + METRICS_ENDPOINT, null, null);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        String status = "";
        if (schema.equals("default")) {
            if (!tableSupplier.get().isEmpty()) {
                Object tableSupplierStatus = tableSupplier.get().get("status");
                if (tableSupplierStatus instanceof String) {
                    status = (String) tableSupplierStatus;
                }
            }

            //TODO prometheus warnings (success|error|warning) could be handled separately
            if (status.equals("success")) {
                List<String> tableNames = (List<String>) tableSupplier.get().get("data");
                if (tableNames == null) {
                    return ImmutableSet.of();
                }
                return ImmutableSet.copyOf(tableNames);
            }
            else {
                if (status.equals("warning")) {
                    log.warn("Prometheus client gets a warning by retrieving table name from metric list");
                }
            }
        }
        throw new PrestoException(PROMETHEUS_TABLES_METRICS_RETRIEVE_ERROR, String.format("Prometheus did no return metrics list (table names): %s", status));
    }

    public PrometheusTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        if (!schema.equals("default")) {
            return null;
        }

        List<String> tableNames = (List<String>) tableSupplier.get().get("data");
        if (tableNames == null) {
            return null;
        }
        if (!tableNames.contains(tableName)) {
            return null;
        }
        return new PrometheusTable(
                tableName,
                ImmutableList.of(
                        new PrometheusColumn("labels", varcharMapType),
                        new PrometheusColumn("timestamp", TIMESTAMP_WITH_TIME_ZONE),
                        new PrometheusColumn("value", DoubleType.DOUBLE)));
    }

    private Map<String, Object> fetchMetrics(JsonCodec<Map<String, Object>> metricsCodec, URI metadataUri)
    {
        return metricsCodec.fromJson(fetchUri(metadataUri));
    }

    public byte[] fetchUri(URI uri)
    {
        Request.Builder requestBuilder = new Request.Builder().url(uri.toString());
        getBearerAuthInfoFromFile().map(bearerToken -> requestBuilder.header("Authorization", "Bearer " + bearerToken));

        Response response;
        try {
            response = httpClient.newCall(requestBuilder.build()).execute();
            if (response.isSuccessful() && response.body() != null) {
                return response.body().bytes();
            }
        }
        catch (IOException e) {
            throw new PrestoException(PROMETHEUS_UNKNOWN_ERROR, "Error reading metrics", e);
        }

        throw new PrestoException(PROMETHEUS_UNKNOWN_ERROR, "Bad response " + response.code() + response.message());
    }

    private Optional<String> getBearerAuthInfoFromFile()
    {
        return bearerTokenFile.map(tokenFileName -> {
            try {
                return Files.readAllLines(tokenFileName.toPath(), UTF_8).toString();
            }
            catch (IOException e) {
                throw new PrestoException(PROMETHEUS_UNKNOWN_ERROR, "Failed to read bearer token file: " + tokenFileName, e);
            }
        });
    }
}
