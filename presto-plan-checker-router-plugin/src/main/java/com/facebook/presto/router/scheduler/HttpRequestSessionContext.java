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
package com.facebook.presto.router.scheduler;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_EXTRA_CREDENTIAL;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_RESOURCE_ESTIMATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_ROLE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.spi.session.ResourceEstimates.CPU_TIME;
import static com.facebook.presto.spi.session.ResourceEstimates.EXECUTION_TIME;
import static com.facebook.presto.spi.session.ResourceEstimates.PEAK_MEMORY;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static java.lang.String.format;
import static java.net.URLDecoder.decode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

public class HttpRequestSessionContext
{
    private static final JsonCodec<SqlFunctionId> SQL_FUNCTION_ID_JSON_CODEC = jsonCodec(SqlFunctionId.class);
    private static final JsonCodec<SqlInvokedFunction> SQL_INVOKED_FUNCTION_JSON_CODEC = jsonCodec(SqlInvokedFunction.class);
    private static final Splitter DOT_SPLITTER = Splitter.on('.');

    private final String catalog;
    private final String schema;

    private final Identity identity;

    private final String source;
    private final String userAgent;
    private final String timeZoneId;
    private final String language;
    private final Set<String> clientTags;
    private final ResourceEstimates resourceEstimates;

    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;

    private final Map<String, String> preparedStatements;

    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final String clientInfo;
    private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;

    private final RuntimeStats runtimeStats = new RuntimeStats();
    private final Map<String, List<String>> headerMap;

    public HttpRequestSessionContext(
            Map<String, List<String>> headerMap,
            SqlParserOptions sqlParserOptions,
            Principal userPrincipal)
    {
        requireNonNull(headerMap, "headerMap is null");
        // Normalize header keys to lowercase
        this.headerMap = normalizeHeaderMap(headerMap);
        catalog = trimEmptyToNull(getHeader(PRESTO_CATALOG));
        schema = trimEmptyToNull(getHeader(PRESTO_SCHEMA));
        assertRequest((catalog != null) || (schema == null), "Schema is set but catalog is not");

        String user = trimEmptyToNull(trimEmptyToNull(getHeader(PRESTO_USER)));
        assertRequest(user != null, "User must be set");
        identity = new Identity(
                user,
                Optional.ofNullable(userPrincipal),
                parseRoleHeaders(headerMap),
                parseExtraCredentials(headerMap),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty());

        source = getHeader(PRESTO_SOURCE);
        userAgent = getHeader(USER_AGENT);
        timeZoneId = getHeader(PRESTO_TIME_ZONE);
        language = getHeader(PRESTO_LANGUAGE);
        clientInfo = getHeader(PRESTO_CLIENT_INFO);
        clientTags = parseClientTags();
        resourceEstimates = parseResourceEstimate(headerMap);

        this.systemProperties = parseSessionProperties(headerMap);
        this.catalogSessionProperties = parseCatalogSessionProperties(headerMap).entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));

        preparedStatements = parsePreparedStatementsHeaders(headerMap, sqlParserOptions);

        String transactionIdHeader = getHeader(PRESTO_TRANSACTION_ID);
        clientTransactionSupport = transactionIdHeader != null;
        transactionId = parseTransactionId(transactionIdHeader);

        this.sessionFunctions = parseSessionFunctionHeader(headerMap);
        //todo : add tracing abilities
    }

    public static List<String> splitSessionHeader(List<String> headers)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return headers.stream()
                .map(splitter::splitToList)
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    public static Map<String, String> getResourceEstimates(HttpRequestSessionContext sessionContext)
    {
        ImmutableMap.Builder<String, String> resourceEstimates = ImmutableMap.builder();
        ResourceEstimates estimates = sessionContext.getResourceEstimates();
        estimates.getExecutionTime().ifPresent(e -> resourceEstimates.put(EXECUTION_TIME, e.toString()));
        estimates.getCpuTime().ifPresent(e -> resourceEstimates.put(CPU_TIME, e.toString()));
        estimates.getPeakMemory().ifPresent(e -> resourceEstimates.put(PEAK_MEMORY, e.toString()));
        return resourceEstimates.build();
    }

    public static Map<String, String> getSerializedSessionFunctions(HttpRequestSessionContext sessionContext)
    {
        return sessionContext.getSessionFunctions().entrySet().stream()
                .collect(collectingAndThen(
                        toMap(e -> SQL_FUNCTION_ID_JSON_CODEC.toJson(e.getKey()), e -> SQL_INVOKED_FUNCTION_JSON_CODEC.toJson(e.getValue())),
                        ImmutableMap::copyOf));
    }

    public static ResourceEstimates parseResourceEstimate(Map<String, List<String>> headerMap)
    {
        ResourceEstimateBuilder builder = new ResourceEstimateBuilder();
        for (String header : splitSessionHeader(headerMap.getOrDefault(PRESTO_RESOURCE_ESTIMATE, emptyList()))) {
            List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", PRESTO_RESOURCE_ESTIMATE);
            String name = nameValue.get(0);
            String value = nameValue.get(1);

            try {
                switch (name.toUpperCase()) {
                    case ResourceEstimates.EXECUTION_TIME:
                        builder.setExecutionTime(Duration.valueOf(value));
                        break;
                    case ResourceEstimates.CPU_TIME:
                        builder.setCpuTime(Duration.valueOf(value));
                        break;
                    case ResourceEstimates.PEAK_MEMORY:
                        builder.setPeakMemory(DataSize.valueOf(value));
                        break;
                    case ResourceEstimates.PEAK_TASK_MEMORY:
                        builder.setPeakTaskMemory(DataSize.valueOf(value));
                        break;
                    default:
                        throw new IllegalStateException(format("Unsupported resource name %s", name));
                }
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(format("Unsupported format for resource estimate '%s': %s", value, e));
            }
        }
        return builder.build();
    }

    public String getHeader(String header)
    {
        return headerMap.getOrDefault(header.toLowerCase(ROOT), emptyList())
                .stream()
                .findFirst()
                .orElse(null);
    }

    public Identity getIdentity()
    {
        return identity;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getSource()
    {
        return source;
    }

    public String getUserAgent()
    {
        return userAgent;
    }

    public String getClientInfo()
    {
        return clientInfo;
    }

    public Set<String> getClientTags()
    {
        return clientTags;
    }

    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
    }

    public String getTimeZoneId()
    {
        return timeZoneId;
    }

    public String getLanguage()
    {
        return language;
    }

    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return catalogSessionProperties;
    }

    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    public boolean supportClientTransaction()
    {
        return clientTransactionSupport;
    }

    public Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions()
    {
        return sessionFunctions;
    }

    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    private static Map<String, List<String>> normalizeHeaderMap(Map<String, List<String>> headerMap)
    {
        return headerMap.entrySet()
                .stream()
                .collect(toImmutableMap(
                        entry -> entry.getKey().toLowerCase(ROOT),
                        Map.Entry::getValue,
                        (existing, replacement) -> {
                            // Merge list entries for duplicate keys
                            existing.addAll(replacement);
                            return existing;
                        }));
    }

    private static Map<String, String> parseSessionHeaders(Map<String, List<String>> headerMap)
    {
        return parseProperty(headerMap, PRESTO_SESSION);
    }

    private static Map<String, SelectedRole> parseRoleHeaders(Map<String, List<String>> headerMap)
    {
        ImmutableMap.Builder<String, SelectedRole> roles = ImmutableMap.builder();
        for (String header : splitSessionHeader(headerMap.getOrDefault(PRESTO_ROLE, emptyList()))) {
            List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", PRESTO_ROLE);
            roles.put(nameValue.get(0), SelectedRole.valueOf(urlDecode(nameValue.get(1))));
        }
        return roles.build();
    }

    private static Map<String, String> parseExtraCredentials(Map<String, List<String>> headerMap)
    {
        return parseCredentialProperty(headerMap, PRESTO_EXTRA_CREDENTIAL);
    }

    private static Map<String, String> parseProperty(Map<String, List<String>> headerMap, String headerName)
    {
        Map<String, String> properties = new HashMap<>();
        for (String header : splitSessionHeader(headerMap.getOrDefault(headerName, emptyList()))) {
            List<String> nameValue = Splitter.on('=').trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", headerName);
            properties.put(nameValue.get(0), urlDecode(nameValue.get(1)));
        }
        return properties;
    }

    private static Map<String, String> parseCredentialProperty(Map<String, List<String>> headerMap, String headerName)
    {
        Map<String, String> properties = new HashMap<>();
        for (String header : splitSessionHeader(headerMap.getOrDefault(headerName, emptyList()))) {
            List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", headerName);
            properties.put(nameValue.get(0), urlDecode(nameValue.get(1)));
        }
        return properties;
    }

    private static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            throw new IllegalStateException(format(format, args));
        }
    }

    private static Map<String, String> parsePreparedStatementsHeaders(Map<String, List<String>> headerMap, SqlParserOptions sqlParserOptions)
    {
        ImmutableMap.Builder<String, String> preparedStatements = ImmutableMap.builder();
        for (String header : splitSessionHeader(headerMap.getOrDefault(PRESTO_PREPARED_STATEMENT, emptyList()))) {
            List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", PRESTO_PREPARED_STATEMENT);

            String statementName;
            String sqlString;
            try {
                statementName = urlDecode(nameValue.get(0));
                sqlString = urlDecode(nameValue.get(1));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException((format("Invalid %s header: %s", PRESTO_PREPARED_STATEMENT, e.getMessage())));
            }

            // Validate statement
            SqlParser sqlParser = new SqlParser(sqlParserOptions);
            try {
                sqlParser.createStatement(sqlString, new ParsingOptions(AS_DOUBLE /* anything */));
            }
            catch (ParsingException e) {
                throw new IllegalStateException(format("Invalid %s header: %s", PRESTO_PREPARED_STATEMENT, e.getMessage()));
            }

            preparedStatements.put(statementName, sqlString);
        }
        return preparedStatements.build();
    }

    private static Optional<TransactionId> parseTransactionId(String transactionId)
    {
        transactionId = trimEmptyToNull(transactionId);
        if (transactionId == null || transactionId.equalsIgnoreCase("none")) {
            return Optional.empty();
        }
        try {
            return Optional.of(TransactionId.valueOf(transactionId));
        }
        catch (Exception e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    private static Map<SqlFunctionId, SqlInvokedFunction> parseSessionFunctionHeader(Map<String, List<String>> headerMap)
    {
        ImmutableMap.Builder<SqlFunctionId, SqlInvokedFunction> sessionFunctions = ImmutableMap.builder();
        for (String header : splitSessionHeader(headerMap.getOrDefault(PRESTO_SESSION_FUNCTION, emptyList()))) {
            List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", PRESTO_SESSION_FUNCTION);

            String serializedFunctionSignature;
            String serializedFunctionDefinition;
            try {
                serializedFunctionSignature = urlDecode(nameValue.get(0));
                serializedFunctionDefinition = urlDecode(nameValue.get(1));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(format("Invalid %s header: %s", PRESTO_SESSION_FUNCTION, e.getMessage()));
            }
            sessionFunctions.put(SQL_FUNCTION_ID_JSON_CODEC.fromJson(serializedFunctionSignature), SQL_INVOKED_FUNCTION_JSON_CODEC.fromJson(serializedFunctionDefinition));
        }
        return sessionFunctions.build();
    }

    private static Map<String, String> parseSessionProperties(Map<String, List<String>> headerMap)
    {
        // parse session properties
        ImmutableMap.Builder<String, String> systemProperties = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : parseSessionHeaders(headerMap).entrySet()) {
            String fullPropertyName = entry.getKey();
            String propertyValue = entry.getValue();
            List<String> nameParts = DOT_SPLITTER.splitToList(fullPropertyName);
            if (nameParts.size() == 1) {
                String propertyName = nameParts.get(0);

                assertRequest(!propertyName.isEmpty(), "Invalid %s header", PRESTO_SESSION);

                // catalog session properties can not be validated until the transaction has stated, so we delay system property validation also
                systemProperties.put(propertyName, propertyValue);
            }
            else if (nameParts.size() == 2) {
                continue;
            }
            else {
                throw new IllegalStateException(format("Invalid %s header", PRESTO_SESSION));
            }
        }
        return systemProperties.build();
    }

    private static Map<String, Map<String, String>> parseCatalogSessionProperties(Map<String, List<String>> headerMap)
    {
        Map<String, Map<String, String>> catalogSessionProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : parseSessionHeaders(headerMap).entrySet()) {
            String fullPropertyName = entry.getKey();
            String propertyValue = entry.getValue();
            List<String> nameParts = DOT_SPLITTER.splitToList(fullPropertyName);
            if (nameParts.size() == 1) {
                continue;
            }
            if (nameParts.size() == 2) {
                String catalogName = nameParts.get(0);
                String propertyName = nameParts.get(1);

                assertRequest(!catalogName.isEmpty(), "Invalid %s header", PRESTO_SESSION);
                assertRequest(!propertyName.isEmpty(), "Invalid %s header", PRESTO_SESSION);

                // catalog session properties can not be validated until the transaction has stated
                catalogSessionProperties.computeIfAbsent(catalogName, id -> new HashMap<>()).put(propertyName, propertyValue);
            }
            else {
                throw new IllegalStateException(format("Invalid %s header", PRESTO_SESSION));
            }
        }
        return catalogSessionProperties;
    }

    private static String trimEmptyToNull(String value)
    {
        return emptyToNull(nullToEmpty(value).trim());
    }

    private static String urlDecode(String value)
    {
        return decode(value, UTF_8);
    }

    private Set<String> parseClientTags()
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return ImmutableSet.copyOf(splitter.split(nullToEmpty(getHeader(PRESTO_CLIENT_TAGS))));
    }

    public static class ResourceEstimateBuilder
    {
        private Optional<Duration> executionTime = Optional.empty();
        private Optional<Duration> cpuTime = Optional.empty();
        private Optional<DataSize> peakMemory = Optional.empty();
        private Optional<DataSize> peakTaskMemory = Optional.empty();

        public ResourceEstimateBuilder setExecutionTime(Duration executionTime)
        {
            this.executionTime = Optional.of(executionTime);
            return this;
        }

        public ResourceEstimateBuilder setCpuTime(Duration cpuTime)
        {
            this.cpuTime = Optional.of(cpuTime);
            return this;
        }

        public ResourceEstimateBuilder setPeakMemory(DataSize peakMemory)
        {
            this.peakMemory = Optional.of(peakMemory);
            return this;
        }

        public ResourceEstimateBuilder setPeakTaskMemory(DataSize peakTaskMemory)
        {
            this.peakTaskMemory = Optional.of(peakTaskMemory);
            return this;
        }

        public ResourceEstimates build()
        {
            return new ResourceEstimates(executionTime, cpuTime, peakMemory, peakTaskMemory);
        }
    }
}
