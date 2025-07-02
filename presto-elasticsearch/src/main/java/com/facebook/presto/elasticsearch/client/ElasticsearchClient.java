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
package com.facebook.presto.elasticsearch.client;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.SearchType;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.query_dsl.FieldAndFormat;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import co.elastic.clients.transport.rest5_client.low_level.Response;
import co.elastic.clients.transport.rest5_client.low_level.ResponseException;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import co.elastic.clients.transport.rest5_client.low_level.Rest5ClientBuilder;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.security.pem.PemReader;
import com.facebook.presto.elasticsearch.AwsSecurityConfig;
import com.facebook.presto.elasticsearch.ElasticsearchConfig;
import com.facebook.presto.elasticsearch.PasswordConfig;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CONNECTION_ERROR;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_INVALID_RESPONSE;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_QUERY_FAILURE;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_SSL_INITIALIZATION_FAILURE;
import static com.facebook.presto.elasticsearch.client.ElasticSearchClientUtils.performRequest;
import static com.facebook.presto.elasticsearch.client.ElasticSearchClientUtils.search;
import static com.facebook.presto.elasticsearch.client.ElasticSearchClientUtils.searchScroll;
import static com.facebook.presto.elasticsearch.client.ElasticSearchClientUtils.setHosts;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.list;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ElasticsearchClient
{
    private static final Logger LOG = Logger.get(ElasticsearchClient.class);
    private static final JsonCodec<SearchShardsResponse> SEARCH_SHARDS_RESPONSE_CODEC = jsonCodec(SearchShardsResponse.class);
    private static final JsonCodec<NodesResponse> NODES_RESPONSE_CODEC = jsonCodec(NodesResponse.class);
    private static final JsonCodec<CountResponse> COUNT_RESPONSE_CODEC = jsonCodec(CountResponse.class);
    private static final ObjectMapper OBJECT_MAPPER = new JsonObjectMapperProvider().get();
    private static final Pattern ADDRESS_PATTERN = Pattern.compile("((?<cname>[^/]+)/)?(?<ip>.+):(?<port>\\d+)");

    private final Rest5Client client;
    private final int scrollSize;
    private final Duration scrollTimeout;

    private final AtomicReference<Set<ElasticsearchNode>> nodes = new AtomicReference<>(ImmutableSet.of());
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("NodeRefresher"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final Duration refreshInterval;
    private final boolean tlsEnabled;
    private final boolean ignorePublishAddress;

    @Inject
    public ElasticsearchClient(
            ElasticsearchConfig config,
            Optional<AwsSecurityConfig> awsSecurityConfig,
            Optional<PasswordConfig> passwordConfig)
    {
        client = createClient(config, awsSecurityConfig, passwordConfig);

        this.ignorePublishAddress = config.isIgnorePublishAddress();
        this.scrollSize = config.getScrollSize();
        this.scrollTimeout = config.getScrollTimeout();
        this.refreshInterval = config.getNodeRefreshInterval();
        this.tlsEnabled = config.isTlsEnabled();
    }

    @PostConstruct
    public void initialize()
    {
        if (!started.getAndSet(true)) {
            // do the first refresh eagerly
            refreshNodes();
            executor.scheduleWithFixedDelay(this::refreshNodes, refreshInterval.toMillis(), refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @PreDestroy
    public void close()
            throws IOException
    {
        executor.shutdownNow();
        client.close();
    }

    private void refreshNodes()
    {
        // discover other nodes in the cluster and add them to the client
        try {
            Set<ElasticsearchNode> nodes = fetchNodes();

            HttpHost[] hosts = nodes.stream()
                    .map(ElasticsearchNode::getAddress)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(address -> {
                        try {
                            return HttpHost.create(format("%s://%s", tlsEnabled ? "https" : "http", address));
                        }
                        catch (URISyntaxException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toArray(HttpHost[]::new);

            if (hosts.length > 0 && !ignorePublishAddress) {
                setHosts(client, hosts);
            }
            this.nodes.set(nodes);
        }
        catch (Throwable e) {
            // Catch all exceptions here since throwing an exception from executor#scheduleWithFixedDelay method
            // suppresses all future scheduled invocations
            LOG.error(e, "Error refreshing nodes");
        }
    }

    private static Rest5Client createClient(
            ElasticsearchConfig config,
            Optional<AwsSecurityConfig> awsSecurityConfig,
            Optional<PasswordConfig> passwordConfig)
    {
        Rest5ClientBuilder builder = Rest5Client.builder(new HttpHost(config.isTlsEnabled() ? "https" : "http", config.getHost(), config.getPort()));
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(toIntExact(config.getConnectTimeout().toMillis())))
                .setConnectionRequestTimeout(Timeout.ofMilliseconds(toIntExact(config.getRequestTimeout().toMillis())))
                .build();

        IOReactorConfig reactorConfig = IOReactorConfig.custom()
                .setIoThreadCount(config.getHttpThreadCount())
                .build();

        // the client builder passed to the call-back is configured to use system properties, which makes it
        // impossible to configure concurrency settings, so we need to build a new one from scratch
        HttpAsyncClientBuilder clientBuilder = HttpAsyncClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .setIOReactorConfig(reactorConfig);

        RegistryBuilder<TlsStrategy> tlsStrategyRegistryBuilder = RegistryBuilder.<TlsStrategy>create();
        if (config.isTlsEnabled()) {
            HostnameVerifier verifier = HttpsSupport.getDefaultHostnameVerifier();
            if (config.isVerifyHostnames()) {
                verifier = NoopHostnameVerifier.INSTANCE;
            }
            DefaultClientTlsStrategy strategy = new DefaultClientTlsStrategy(
                    buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTrustStorePath(), config.getTruststorePassword()).get(), verifier);
            tlsStrategyRegistryBuilder.register("https", strategy);
        }

        PoolingAsyncClientConnectionManager poolingAsyncClientConnectionManager = new PoolingAsyncClientConnectionManager(tlsStrategyRegistryBuilder.build());
        poolingAsyncClientConnectionManager.setMaxTotal(config.getMaxHttpConnections());
        clientBuilder.setConnectionManager(poolingAsyncClientConnectionManager);

        passwordConfig.ifPresent(securityConfig -> {
            BasicCredentialsProvider credentials = new BasicCredentialsProvider();
            credentials.setCredentials(new AuthScope("ANY", 12), new UsernamePasswordCredentials(securityConfig.getUser(), securityConfig.getPassword().toCharArray()));
            clientBuilder.setDefaultCredentialsProvider(credentials);
        });

        awsSecurityConfig.ifPresent(securityConfig -> clientBuilder.addRequestInterceptorLast(new AwsRequestSigner(
                securityConfig.getRegion(),
                getAwsCredentialsProvider(securityConfig))));

        builder.setHttpClient(clientBuilder.build());

        return builder.build();
    }

    private static AWSCredentialsProvider getAwsCredentialsProvider(AwsSecurityConfig config)
    {
        if (config.getAccessKey().isPresent() && config.getSecretKey().isPresent()) {
            return new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                    config.getAccessKey().get(),
                    config.getSecretKey().get()));
        }
        if (config.isUseInstanceCredentials()) {
            return InstanceProfileCredentialsProvider.getInstance();
        }
        return DefaultAWSCredentialsProviderChain.getInstance();
    }

    private static Optional<SSLContext> buildSslContext(
            Optional<File> keyStorePath,
            Optional<String> keyStorePassword,
            Optional<File> trustStorePath,
            Optional<String> trustStorePassword)
    {
        if (!keyStorePath.isPresent() && !trustStorePath.isPresent()) {
            return Optional.empty();
        }

        try {
            // load KeyStore if configured and get KeyManagers
            KeyStore keyStore = null;
            KeyManager[] keyManagers = null;
            if (keyStorePath.isPresent()) {
                char[] keyManagerPassword;
                try {
                    // attempt to read the key store as a PEM file
                    keyStore = PemReader.loadKeyStore(keyStorePath.get(), keyStorePath.get(), keyStorePassword);
                    // for PEM encoded keys, the password is used to decrypt the specific key (and does not protect the keystore itself)
                    keyManagerPassword = new char[0];
                }
                catch (IOException | GeneralSecurityException ignored) {
                    keyManagerPassword = keyStorePassword.map(String::toCharArray).orElse(null);

                    keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                    try (InputStream in = new FileInputStream(keyStorePath.get())) {
                        keyStore.load(in, keyManagerPassword);
                    }
                }

                validateCertificates(keyStore);
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, keyManagerPassword);
                keyManagers = keyManagerFactory.getKeyManagers();
            }

            // load TrustStore if configured, otherwise use KeyStore
            KeyStore trustStore = keyStore;
            if (trustStorePath.isPresent()) {
                trustStore = loadTrustStore(trustStorePath.get(), trustStorePassword);
            }

            // create TrustManagerFactory
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            // get X509TrustManager
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if ((trustManagers.length != 1) || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new RuntimeException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
            }
            X509TrustManager trustManager = (X509TrustManager) trustManagers[0];

            // create SSLContext
            SSLContext result = SSLContext.getInstance("TLS");
            result.init(keyManagers, new TrustManager[] {trustManager}, null);
            return Optional.of(result);
        }
        catch (GeneralSecurityException | IOException e) {
            throw new PrestoException(ELASTICSEARCH_SSL_INITIALIZATION_FAILURE, e);
        }
    }

    private static KeyStore loadTrustStore(File trustStorePath, Optional<String> trustStorePassword)
            throws IOException, GeneralSecurityException
    {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            // attempt to read the trust store as a PEM file
            List<X509Certificate> certificateChain = PemReader.readCertificateChain(trustStorePath);
            if (!certificateChain.isEmpty()) {
                trustStore.load(null, null);
                for (X509Certificate certificate : certificateChain) {
                    X500Principal principal = certificate.getSubjectX500Principal();
                    trustStore.setCertificateEntry(principal.getName(), certificate);
                }
                return trustStore;
            }
        }
        catch (IOException | GeneralSecurityException ignored) {
        }

        try (InputStream in = new FileInputStream(trustStorePath)) {
            trustStore.load(in, trustStorePassword.map(String::toCharArray).orElse(null));
        }
        return trustStore;
    }

    private static void validateCertificates(KeyStore keyStore)
            throws GeneralSecurityException
    {
        for (String alias : list(keyStore.aliases())) {
            if (!keyStore.isKeyEntry(alias)) {
                continue;
            }
            Certificate certificate = keyStore.getCertificate(alias);
            if (!(certificate instanceof X509Certificate)) {
                continue;
            }
            try {
                ((X509Certificate) certificate).checkValidity();
            }
            catch (CertificateExpiredException e) {
                throw new CertificateExpiredException("KeyStore certificate is expired: " + e.getMessage());
            }
            catch (CertificateNotYetValidException e) {
                throw new CertificateNotYetValidException("KeyStore certificate is not yet valid: " + e.getMessage());
            }
        }
    }

    private Set<ElasticsearchNode> fetchNodes()
    {
        NodesResponse nodesResponse = doRequest("/_nodes/http", NODES_RESPONSE_CODEC::fromJson);

        ImmutableSet.Builder<ElasticsearchNode> result = ImmutableSet.builder();
        for (Map.Entry<String, NodesResponse.Node> entry : nodesResponse.getNodes().entrySet()) {
            String nodeId = entry.getKey();
            NodesResponse.Node node = entry.getValue();

            if (node.getRoles().contains("data")) {
                Optional<String> address = node.getAddress()
                        .flatMap(ElasticsearchClient::extractAddress);

                result.add(new ElasticsearchNode(nodeId, address));
            }
        }
        return result.build();
    }

    public Set<ElasticsearchNode> getNodes()
    {
        return nodes.get();
    }

    public List<Shard> getSearchShards(String index)
    {
        Map<String, ElasticsearchNode> nodeById = getNodes().stream()
                .collect(toImmutableMap(ElasticsearchNode::getId, Function.identity()));

        SearchShardsResponse shardsResponse = doRequest(format("/%s/_search_shards", index), SEARCH_SHARDS_RESPONSE_CODEC::fromJson);

        ImmutableList.Builder<Shard> shards = ImmutableList.builder();
        List<ElasticsearchNode> nodes = ImmutableList.copyOf(nodeById.values());

        for (List<SearchShardsResponse.Shard> shardGroup : shardsResponse.getShardGroups()) {
            Stream<SearchShardsResponse.Shard> preferred = shardGroup.stream()
                    .sorted(this::shardPreference);

            Optional<SearchShardsResponse.Shard> candidate = preferred
                    .filter(shard -> shard.getNode() != null && nodeById.containsKey(shard.getNode()))
                    .findFirst();

            SearchShardsResponse.Shard chosen;
            ElasticsearchNode node;
            if (candidate.isPresent()) {
                chosen = candidate.get();
                node = nodeById.get(chosen.getNode());
            }
            else {
                // pick an arbitrary shard with and assign to an arbitrary node
                chosen = preferred.findFirst().get();
                node = nodes.get(chosen.getShard() % nodes.size());
            }
            shards.add(new Shard(chosen.getIndex(), chosen.getShard(), node.getAddress()));
        }

        return shards.build();
    }

    private int shardPreference(SearchShardsResponse.Shard left, SearchShardsResponse.Shard right)
    {
        // Favor non-primary shards
        if (left.isPrimary() == right.isPrimary()) {
            return 0;
        }

        return left.isPrimary() ? 1 : -1;
    }

    public List<String> getIndexes()
    {
        return doRequest("/_cat/indices?h=index&format=json&s=index:asc", body -> {
            try {
                ImmutableList.Builder<String> result = ImmutableList.builder();
                JsonNode root = OBJECT_MAPPER.readTree(body);
                for (int i = 0; i < root.size(); i++) {
                    result.add(root.get(i).get("index").asText());
                }
                return result.build();
            }
            catch (IOException e) {
                throw new PrestoException(ELASTICSEARCH_INVALID_RESPONSE, e);
            }
        });
    }

    public Map<String, List<String>> getAliases()
    {
        return doRequest("/_aliases", body -> {
            try {
                ImmutableMap.Builder<String, List<String>> result = ImmutableMap.builder();
                JsonNode root = OBJECT_MAPPER.readTree(body);

                Iterator<Map.Entry<String, JsonNode>> elements = root.fields();
                while (elements.hasNext()) {
                    Map.Entry<String, JsonNode> element = elements.next();
                    JsonNode aliases = element.getValue().get("aliases");
                    Iterator<String> aliasNames = aliases.fieldNames();
                    if (aliasNames.hasNext()) {
                        result.put(element.getKey(), ImmutableList.copyOf(aliasNames));
                    }
                }
                return result.build();
            }
            catch (IOException e) {
                throw new PrestoException(ELASTICSEARCH_INVALID_RESPONSE, e);
            }
        });
    }

    public IndexMetadata getIndexMetadata(String index)
    {
        String path = format("/%s/_mappings", index);

        return doRequest(path, body -> {
            try {
                JsonNode mappings = OBJECT_MAPPER.readTree(body)
                        .elements().next()
                        .get("mappings");

                if (!mappings.has("properties")) {
                    // Older versions of ElasticSearch supported multiple "type" mappings
                    // for a given index. Newer versions support only one and don't
                    // expose it in the document. Here we skip it if it's present.

                    if (!mappings.elements().hasNext()) {
                        return new IndexMetadata(new IndexMetadata.ObjectType(ImmutableList.of()));
                    }
                    mappings = mappings.elements().next();
                }

                JsonNode metaNode = nullSafeNode(mappings, "_meta");

                return new IndexMetadata(parseType(mappings.get("properties"), nullSafeNode(metaNode, "presto")));
            }
            catch (IOException e) {
                throw new PrestoException(ELASTICSEARCH_INVALID_RESPONSE, e);
            }
            catch (NoSuchElementException e) {
                throw new PrestoException(ELASTICSEARCH_INVALID_RESPONSE, "No mappings found for index: " + index);
            }
        });
    }

    private IndexMetadata.ObjectType parseType(JsonNode properties, JsonNode metaProperties)
    {
        Iterator<Map.Entry<String, JsonNode>> entries = properties.fields();

        ImmutableList.Builder<IndexMetadata.Field> result = ImmutableList.builder();
        while (entries.hasNext()) {
            Map.Entry<String, JsonNode> field = entries.next();

            String name = field.getKey();
            JsonNode value = field.getValue();

            String type = "object";
            if (value.has("type")) {
                type = value.get("type").asText();
            }

            JsonNode metaNode = nullSafeNode(metaProperties, name);
            boolean isArray = !metaNode.isNull() && metaNode.has("isArray") && metaNode.get("isArray").asBoolean();

            switch (type) {
                case "date":
                    List<String> formats = ImmutableList.of();
                    if (value.has("format")) {
                        formats = Arrays.asList(value.get("format").asText().split("\\|\\|"));
                    }
                    result.add(new IndexMetadata.Field(isArray, name, new IndexMetadata.DateTimeType(formats)));
                    break;

                case "nested":
                case "object":
                    if (value.has("properties")) {
                        result.add(new IndexMetadata.Field(isArray, name, parseType(value.get("properties"), metaNode)));
                    }
                    else {
                        LOG.debug("Ignoring empty object field: %s", name);
                    }
                    break;

                default:
                    result.add(new IndexMetadata.Field(isArray, name, new IndexMetadata.PrimitiveType(type)));
            }
        }

        return new IndexMetadata.ObjectType(result.build());
    }

    private JsonNode nullSafeNode(JsonNode jsonNode, String name)
    {
        if (jsonNode == null || jsonNode.isNull() || jsonNode.get(name) == null) {
            return NullNode.getInstance();
        }
        return jsonNode.get(name);
    }

    public String executeQuery(String index, String query)
    {
        String path = format("/%s/_search", index);

        Response response;
        try {
            response = performRequest(
                            "GET",
                            path,
                            ImmutableMap.of(),
                            new ByteArrayEntity(query.getBytes(UTF_8), ContentType.APPLICATION_JSON),
                            client,
                            new BasicHeader("Content-Type", "application/json"),
                            new BasicHeader("Accept-Encoding", "application/json"));
        }
        catch (IOException e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }

        String body;
        try {
            body = EntityUtils.toString(response.getEntity());
        }
        catch (IOException | ParseException e) {
            throw new PrestoException(ELASTICSEARCH_INVALID_RESPONSE, e);
        }

        return body;
    }

    public SearchResponse beginSearch(String index, int shard, Query query, Optional<List<String>> fields, List<FieldAndFormat> documentFields, Optional<SortOptions> sort)
    {
        SourceConfig.Builder sourceConfigBuilder = new SourceConfig.Builder();

        fields.ifPresent(values -> {
            if (values.isEmpty()) {
                sourceConfigBuilder.fetch(false);
            }
            else {
                sourceConfigBuilder.filter(fb -> fb.includes(values));
            }
        });

        SearchRequest.Builder requestBuilder = new SearchRequest.Builder()
                .index(index)
                .searchType(SearchType.QueryThenFetch)
                .preference("_shards:" + shard)
                .scroll(new Time.Builder().time(String.valueOf(scrollTimeout.toMillis())).build())
                .size(scrollSize)
                .query(query)
                .source(sourceConfigBuilder.build())
                .docvalueFields(documentFields);

        sort.ifPresent(requestBuilder::sort);
        documentFields.forEach(requestBuilder::docvalueFields);

        try {
            return search(requestBuilder.build(), client);
        }
        catch (IOException e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
        catch (ElasticsearchException e) {
            Throwable[] suppressed = e.getSuppressed();
            if (suppressed.length > 0) {
                Throwable cause = suppressed[0];
                if (cause instanceof ResponseException) {
                    HttpEntity entity = ((ResponseException) cause).getResponse().getEntity();
                    try {
                        JsonNode reason = OBJECT_MAPPER.readTree(entity.getContent()).path("error")
                                .path("root_cause")
                                .path(0)
                                .path("reason");

                        if (!reason.isMissingNode()) {
                            throw new PrestoException(ELASTICSEARCH_QUERY_FAILURE, reason.asText(), e);
                        }
                    }
                    catch (IOException ex) {
                        e.addSuppressed(ex);
                    }
                }
            }

            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
    }

    public ScrollResponse<Void> nextPage(String scrollId)
    {
        ScrollRequest request = new ScrollRequest.Builder().scrollId(scrollId)
                .scroll(new Time.Builder().time(String.valueOf(scrollTimeout.toMillis())).build()).build();

        try {
            return searchScroll(request, client);
        }
        catch (IOException e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
    }

    public long count(String index, int shard, Query query)
    {
        CountRequest.Builder countRequestBuilder = new CountRequest.Builder().index(index).query(query)
                .preference(format("_shards:%s", shard));

        try {
            long count = ElasticSearchClientUtils.count(countRequestBuilder.build(), client).count();
            LOG.debug("Count: %s:%s, query: %s", index, shard, count);
            return count;
        }
        catch (IOException e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
    }

    public void clearScroll(String scrollId)
    {
        ClearScrollRequest request = new ClearScrollRequest.Builder().scrollId(scrollId).build();
        try {
            ElasticSearchClientUtils.clearScroll(request, client);
        }
        catch (IOException e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
    }

    private <T> T doRequest(String path, ResponseHandler<T> handler)
    {
        checkArgument(path.startsWith("/"), "path must be an absolute path");

        Response response;
        try {
            response = performRequest("GET", path, client);
        }
        catch (IOException e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }

        String body;
        try {
            body = EntityUtils.toString(response.getEntity());
        }
        catch (IOException | ParseException e) {
            throw new PrestoException(ELASTICSEARCH_INVALID_RESPONSE, e);
        }
        return handler.process(body);
    }

    private static PrestoException propagate(ResponseException exception)
    {
        HttpEntity entity = exception.getResponse().getEntity();

        if (entity != null && entity.getContentType() != null) {
            try {
                JsonNode reason = OBJECT_MAPPER.readTree(entity.getContent()).path("error")
                        .path("root_cause")
                        .path(0)
                        .path("reason");

                if (!reason.isMissingNode()) {
                    throw new PrestoException(ELASTICSEARCH_QUERY_FAILURE, reason.asText(), exception);
                }
            }
            catch (IOException e) {
                PrestoException result = new PrestoException(ELASTICSEARCH_QUERY_FAILURE, exception);
                result.addSuppressed(e);
                throw result;
            }
        }

        throw new PrestoException(ELASTICSEARCH_QUERY_FAILURE, exception);
    }

    @VisibleForTesting
    static Optional<String> extractAddress(String address)
    {
        Matcher matcher = ADDRESS_PATTERN.matcher(address);

        if (!matcher.matches()) {
            return Optional.empty();
        }

        String cname = matcher.group("cname");
        String ip = matcher.group("ip");
        String port = matcher.group("port");

        if (cname != null) {
            return Optional.of(cname + ":" + port);
        }

        return Optional.of(ip + ":" + port);
    }

    private interface ResponseHandler<T>
    {
        T process(String body);
    }
}
