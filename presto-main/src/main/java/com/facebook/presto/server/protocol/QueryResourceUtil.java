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
package com.facebook.presto.server.protocol;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.ParameterKind;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.CacheControl;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREFIX_URL;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_REMOVED_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_ROLE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_PREREQUISITES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public final class QueryResourceUtil
{
    private static final Logger log = Logger.get(QueryResourceUtil.class);
    public static final String X_FORWARDED_HOST = "X-Forwarded-Host";
    public static final String X_FORWARDED_PORT = "X-Forwarded-Port";
    private static final Splitter FORWARDED_HEADER_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private static final JsonCodec<SqlFunctionId> SQL_FUNCTION_ID_JSON_CODEC = jsonCodec(SqlFunctionId.class);
    private static final JsonCodec<SqlInvokedFunction> SQL_INVOKED_FUNCTION_JSON_CODEC = jsonCodec(SqlInvokedFunction.class);
    private static final JsonCodec<List<Object>> LIST_JSON_CODEC = listJsonCodec(Object.class);
    private static final JsonCodec<Map<Object, Object>> MAP_JSON_CODEC = mapJsonCodec(Object.class, Object.class);
    static final Duration NO_DURATION = new Duration(0, MILLISECONDS);

    private QueryResourceUtil() {}

    public static Response toResponse(Query query, QueryResults queryResults, boolean compressionEnabled, long durationUntilExpirationMs)
    {
        Response.ResponseBuilder response = Response.ok(queryResults);

        // add set catalog and schema
        query.getSetCatalog().ifPresent(catalog -> response.header(PRESTO_SET_CATALOG, catalog));
        query.getSetSchema().ifPresent(schema -> response.header(PRESTO_SET_SCHEMA, schema));

        // add set session properties
        query.getSetSessionProperties()
                .forEach((key, value) -> response.header(PRESTO_SET_SESSION, key + '=' + urlEncode(value)));

        // add clear session properties
        query.getResetSessionProperties()
                .forEach(name -> response.header(PRESTO_CLEAR_SESSION, name));

        // add set roles
        query.getSetRoles()
                .forEach((key, value) -> response.header(PRESTO_SET_ROLE, key + '=' + urlEncode(value.toString())));

        // add added prepare statements
        for (Map.Entry<String, String> entry : query.getAddedPreparedStatements().entrySet()) {
            String encodedKey = urlEncode(entry.getKey());
            String encodedValue = urlEncode(entry.getValue());
            response.header(PRESTO_ADDED_PREPARE, encodedKey + '=' + encodedValue);
        }

        // add deallocated prepare statements
        for (String name : query.getDeallocatedPreparedStatements()) {
            response.header(PRESTO_DEALLOCATED_PREPARE, urlEncode(name));
        }

        // add new transaction ID
        query.getStartedTransactionId()
                .ifPresent(transactionId -> response.header(PRESTO_STARTED_TRANSACTION_ID, transactionId));

        // add clear transaction ID directive
        if (query.isClearTransactionId()) {
            response.header(PRESTO_CLEAR_TRANSACTION_ID, true);
        }

        if (!compressionEnabled) {
            response.encoding("identity");
        }

        // add added session functions
        for (Map.Entry<SqlFunctionId, SqlInvokedFunction> entry : query.getAddedSessionFunctions().entrySet()) {
            response.header(PRESTO_ADDED_SESSION_FUNCTION, format(
                    "%s=%s",
                    urlEncode(SQL_FUNCTION_ID_JSON_CODEC.toJson(entry.getKey())),
                    urlEncode(SQL_INVOKED_FUNCTION_JSON_CODEC.toJson(entry.getValue()))));
        }

        // add removed session functions
        for (SqlFunctionId signature : query.getRemovedSessionFunctions()) {
            response.header(PRESTO_REMOVED_SESSION_FUNCTION, urlEncode(SQL_FUNCTION_ID_JSON_CODEC.toJson(signature)));
        }

        response.cacheControl(getCacheControlMaxAge(durationUntilExpirationMs));

        return response.build();
    }

    public static Response toResponse(
            Query query,
            QueryResults queryResults,
            String xPrestoPrefixUri,
            boolean compressionEnabled,
            boolean nestedDataSerializationEnabled,
            long durationUntilExpirationMs)
    {
        Iterable<List<Object>> queryResultsData = queryResults.getData();
        if (nestedDataSerializationEnabled) {
            queryResultsData = prepareJsonData(queryResults.getColumns(), queryResultsData);
        }
        QueryResults resultsClone = new QueryResults(
                queryResults.getId(),
                prependUri(queryResults.getInfoUri(), xPrestoPrefixUri),
                prependUri(queryResults.getPartialCancelUri(), xPrestoPrefixUri),
                prependUri(queryResults.getNextUri(), xPrestoPrefixUri),
                queryResults.getColumns(),
                queryResultsData,
                queryResults.getBinaryData(),
                queryResults.getStats(),
                queryResults.getError(),
                queryResults.getWarnings(),
                queryResults.getUpdateType(),
                queryResults.getUpdateCount());

        return toResponse(query, resultsClone, compressionEnabled, durationUntilExpirationMs);
    }

    public static CacheControl getCacheControlMaxAge(long durationUntilExpirationMs)
    {
        return CacheControl.valueOf("max-age=" + MILLISECONDS.toSeconds(durationUntilExpirationMs));
    }

    public static void abortIfPrefixUrlInvalid(String xPrestoPrefixUrl)
    {
        if (xPrestoPrefixUrl != null) {
            try {
                URL url = new URL(xPrestoPrefixUrl);
            }
            catch (java.net.MalformedURLException e) {
                throw new WebApplicationException(
                        Response.status(Response.Status.BAD_REQUEST)
                                .type(TEXT_PLAIN_TYPE)
                                .entity(PRESTO_PREFIX_URL + " is not a valid URL")
                                .build());
            }
        }
    }

    public static URI prependUri(URI backendUri, String xPrestoPrefixUrl)
    {
        if (!isNullOrEmpty(xPrestoPrefixUrl) && (backendUri != null)) {
            String encodedBackendUri = Base64.getUrlEncoder().encodeToString(backendUri.toASCIIString().getBytes());

            try {
                return new URI(xPrestoPrefixUrl + encodedBackendUri);
            }
            catch (URISyntaxException e) {
                log.error(e, "Unable to add Proxy Prefix to URL");
            }
        }

        return backendUri;
    }

    public static ExternalUriInfo createExternalUriInfo(UriInfo uriInfo, String xForwardedProto, String xForwardedHost, String xForwardedPort, boolean useForwardedHeaders)
    {
        String scheme = getScheme(xForwardedProto, uriInfo);
        if (!useForwardedHeaders || isNullOrEmpty(xForwardedHost)) {
            return new ExternalUriInfo(scheme, Optional.empty(), OptionalInt.empty());
        }

        try {
            HostAndPort forwardedHost = HostAndPort.fromString(getFirstForwardedHeaderValue(xForwardedHost));
            OptionalInt forwardedPort = parseForwardedPort(forwardedHost, xForwardedPort);
            return new ExternalUriInfo(scheme, Optional.of(forwardedHost.getHost()), forwardedPort);
        }
        catch (RuntimeException e) {
            log.warn(e, "Ignoring invalid forwarded host or port headers while building statement response URIs");
            return new ExternalUriInfo(scheme, Optional.empty(), OptionalInt.empty());
        }
    }

    public static UriBuilder getExternalUriBuilder(UriInfo uriInfo, ExternalUriInfo externalUriInfo)
    {
        UriBuilder uriBuilder = uriInfo.getBaseUriBuilder()
                .scheme(externalUriInfo.getScheme());

        if (externalUriInfo.getHost().isPresent()) {
            uriBuilder.host(externalUriInfo.getHost().get());
            if (externalUriInfo.getPort().isPresent()) {
                uriBuilder.port(externalUriInfo.getPort().getAsInt());
            }
            else {
                uriBuilder.port(-1);
            }
        }

        return uriBuilder;
    }

    public static String getRequestHost(UriInfo uriInfo, ExternalUriInfo externalUriInfo)
    {
        return externalUriInfo.getHost().orElseGet(() -> uriInfo.getBaseUri().getHost());
    }

    private static OptionalInt parseForwardedPort(HostAndPort forwardedHost, String xForwardedPort)
    {
        if (forwardedHost.hasPort()) {
            return OptionalInt.of(forwardedHost.getPort());
        }

        if (isNullOrEmpty(xForwardedPort)) {
            return OptionalInt.empty();
        }

        try {
            return OptionalInt.of(Integer.parseInt(getFirstForwardedHeaderValue(xForwardedPort)));
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid X-Forwarded-Port header", e);
        }
    }

    private static String getFirstForwardedHeaderValue(String headerValue)
    {
        return FORWARDED_HEADER_SPLITTER.splitToList(headerValue).get(0);
    }

    private static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Problem: As the type of data defined in QueryResult is `Iterable<List<Object>>`,
     * when jackson serialize a data with nested data structure in the response, the nested object won't
     * be serialized but be printed as string. For example the map will be printed as "{1=2}", which cannot
     * be recognized and deserialized by client side.
     * Solution: We pre-serialize the data nested in the Object to JSON by following parseTypeSignature,
     * only Objects contains nested structures will be pre-serialized, otherwise the object will simply
     * be returned. Then we can deserialize the JSON in the client side.
     */
    private static Iterable<List<Object>> prepareJsonData(List<Column> columns, Iterable<List<Object>> data)
    {
        if (data == null) {
            return null;
        }
        requireNonNull(columns, "columns is null");
        List<TypeSignature> signatures = columns.stream()
                .map(column -> parseTypeSignature(column.getType()))
                .collect(toList());
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        for (List<Object> row : data) {
            checkArgument(row.size() == columns.size(), "row/column size mismatch");
            List<Object> newRow = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                newRow.add(parseToJson(signatures.get(i), row.get(i)));
            }
            rows.add(unmodifiableList(newRow)); // allow nulls in list
        }
        return rows.build();
    }

    public static Object parseToJson(TypeSignature signature, Object value)
    {
        if (value == null) {
            return null;
        }
        if (signature.isDistinctType()) {
            return parseToJson(signature.getDistinctTypeInfo().getBaseType(), value);
        }
        if (signature.getBase().equals(ARRAY)) {
            List<Object> parsedValue = new ArrayList<>();
            for (Object object : List.class.cast(value)) {
                parsedValue.add(parseToJson(signature.getTypeOrNamedTypeParametersAsTypeSignatures().get(0), object));
            }
            return LIST_JSON_CODEC.toJson(parsedValue);
        }
        if (signature.getBase().equals(MAP)) {
            TypeSignature keySignature = signature.getTypeOrNamedTypeParametersAsTypeSignatures().get(0);
            TypeSignature valueSignature = signature.getTypeOrNamedTypeParametersAsTypeSignatures().get(1);
            Map<Object, Object> parsedValue = new HashMap<>(Map.class.cast(value).size());
            for (Map.Entry<?, ?> entry : (Set<Map.Entry<?, ?>>) Map.class.cast(value).entrySet()) {
                parsedValue.put(parseToJson(keySignature, entry.getKey()), parseToJson(valueSignature, entry.getValue()));
            }
            return MAP_JSON_CODEC.toJson(parsedValue);
        }
        if (signature.getBase().equals(ROW)) {
            List<Object> parsedValue = new ArrayList<>();
            for (int i = 0; i < List.class.cast(value).size(); i++) {
                Object object = List.class.cast(value).get(i);
                TypeSignatureParameter parameter = signature.getParameters().get(i);
                checkArgument(
                        parameter.getKind() == ParameterKind.NAMED_TYPE,
                        "Unexpected parameter [%s] for row type",
                        parameter);
                NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                parsedValue.add(parseToJson(namedTypeSignature.getTypeSignature(), object));
            }
            return LIST_JSON_CODEC.toJson(parsedValue);
        }
        return value;
    }

    private static URI getQueryHtmlUri(QueryId queryId, UriInfo uriInfo, ExternalUriInfo externalUriInfo, String xPrestoPrefixUrl)
    {
        URI uri = getExternalUriBuilder(uriInfo, externalUriInfo)
                .replacePath("ui/query.html")
                .replaceQuery(queryId.toString())
                .build();
        return prependUri(uri, xPrestoPrefixUrl);
    }

    public static URI getQueuedUri(QueryId queryId, String slug, long token, UriInfo uriInfo, ExternalUriInfo externalUriInfo, String xPrestoPrefixUrl, boolean binaryResults)
    {
        UriBuilder uriBuilder = getExternalUriBuilder(uriInfo, externalUriInfo)
                .replacePath("/v1/statement/queued")
                .path(queryId.toString())
                .path(String.valueOf(token))
                .replaceQuery("")
                .queryParam("slug", slug);
        if (binaryResults) {
            uriBuilder.queryParam("binaryResults", "true");
        }
        URI uri = uriBuilder.build();
        return prependUri(uri, xPrestoPrefixUrl);
    }

    public static String getScheme(String xForwardedProto, @Context UriInfo uriInfo)
    {
        return isNullOrEmpty(xForwardedProto) ? uriInfo.getRequestUri().getScheme() : xForwardedProto;
    }

    public static QueryResults createQueuedQueryResults(
            QueryId queryId,
            URI nextUri,
            Optional<QueryError> queryError,
            UriInfo uriInfo,
            ExternalUriInfo externalUriInfo,
            String xPrestoPrefixUrl,
            Duration elapsedTime,
            Optional<Duration> queuedTime,
            Duration waitingForPrerequisitesTime)
    {
        QueryState state = queryError.map(error -> FAILED)
                .orElseGet(() -> queuedTime.isPresent() ? QUEUED : WAITING_FOR_PREREQUISITES);
        return new QueryResults(
                queryId.toString(),
                getQueryHtmlUri(queryId, uriInfo, externalUriInfo, xPrestoPrefixUrl),
                null,
                nextUri,
                null,
                null,
                null,
                StatementStats.builder()
                        .setState(state.toString())
                        .setWaitingForPrerequisites(state == WAITING_FOR_PREREQUISITES)
                        .setElapsedTimeMillis(elapsedTime.toMillis())
                        .setQueuedTimeMillis(queuedTime.orElse(NO_DURATION).toMillis())
                        .setWaitingForPrerequisitesTimeMillis(waitingForPrerequisitesTime.toMillis())
                        .build(),
                queryError.orElse(null),
                ImmutableList.of(),
                null,
                null);
    }

    public static final class ExternalUriInfo
    {
        private final String scheme;
        private final Optional<String> host;
        private final OptionalInt port;

        private ExternalUriInfo(String scheme, Optional<String> host, OptionalInt port)
        {
            this.scheme = requireNonNull(scheme, "scheme is null");
            this.host = requireNonNull(host, "host is null");
            this.port = requireNonNull(port, "port is null");
        }

        public String getScheme()
        {
            return scheme;
        }

        public Optional<String> getHost()
        {
            return host;
        }

        public OptionalInt getPort()
        {
            return port;
        }
    }
}
