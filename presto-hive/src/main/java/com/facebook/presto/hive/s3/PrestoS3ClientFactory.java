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
package com.facebook.presto.hive.s3;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.hive.HiveClientConfig;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ENDPOINT;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_PIN_CLIENT_TO_CURRENT_REGION;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

/**
 * This factory provides S3Client and S3AsyncClient required for executing S3SelectPushdown requests.
 * Normal S3 GET requests use S3Client instances initialized in PrestoS3FileSystem or EMRFS.
 * The ideal state will be to merge this logic with the two file systems and get rid of this
 * factory class.
 * Please do not use the client provided by this factory for any other use cases.
 */
public class PrestoS3ClientFactory
{
    private static final Logger log = Logger.get(PrestoS3ClientFactory.class);
    private static final String S3_ACCESS_KEY = "presto.s3.access-key";
    private static final String S3_SECRET_KEY = "presto.s3.secret-key";
    private static final String S3_CREDENTIALS_PROVIDER = "presto.s3.credentials-provider";
    private static final String S3_USE_INSTANCE_CREDENTIALS = "presto.s3.use-instance-credentials";
    private static final String S3_CONNECT_TIMEOUT = "presto.s3.connect-timeout";
    private static final String S3_SOCKET_TIMEOUT = "presto.s3.socket-timeout";
    private static final String S3_SSL_ENABLED = "presto.s3.ssl.enabled";
    private static final String S3_MAX_ERROR_RETRIES = "presto.s3.max-error-retries";
    private static final String S3_USER_AGENT_PREFIX = "presto.s3.user-agent-prefix";
    private static final String S3_SELECT_PUSHDOWN_MAX_CONNECTIONS = "hive.s3select-pushdown.max-connections";
    private static String s3UserAgentSuffix = "presto";

    @GuardedBy("this")
    private S3Client s3Client;

    @GuardedBy("this")
    private S3AsyncClient s3AsyncClient;

    synchronized S3Client getS3Client(Configuration config, HiveClientConfig clientConfig)
    {
        if (s3Client != null) {
            return s3Client;
        }

        s3Client = buildS3Client(config, clientConfig);
        return s3Client;
    }

    synchronized S3AsyncClient getS3AsyncClient(Configuration config, HiveClientConfig clientConfig)
    {
        if (s3AsyncClient != null) {
            return s3AsyncClient;
        }

        s3AsyncClient = buildS3AsyncClient(config, clientConfig);
        return s3AsyncClient;
    }

    private S3Client buildS3Client(Configuration config, HiveClientConfig clientConfig)
    {
        HiveS3Config defaults = new HiveS3Config();
        String userAgentPrefix = config.get(S3_USER_AGENT_PREFIX, defaults.getS3UserAgentPrefix());
        int maxErrorRetries = config.getInt(S3_MAX_ERROR_RETRIES, defaults.getS3MaxErrorRetries());
        boolean sslEnabled = config.getBoolean(S3_SSL_ENABLED, defaults.isS3SslEnabled());
        Duration connectTimeout = Duration.valueOf(config.get(S3_CONNECT_TIMEOUT, defaults.getS3ConnectTimeout().toString()));
        Duration socketTimeout = Duration.valueOf(config.get(S3_SOCKET_TIMEOUT, defaults.getS3SocketTimeout().toString()));
        int maxConnections = config.getInt(S3_SELECT_PUSHDOWN_MAX_CONNECTIONS, clientConfig.getS3SelectPushdownMaxConnections());

        if (clientConfig.isS3SelectPushdownEnabled()) {
            s3UserAgentSuffix = "presto-select";
        }

        AwsCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(config, defaults);

        S3ClientBuilder clientBuilder = S3Client.builder()
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(builder -> builder
                        .retryPolicy(retryPolicyBuilder -> retryPolicyBuilder
                                .numRetries(maxErrorRetries))
                        .apiCallTimeout(java.time.Duration.ofMillis(socketTimeout.toMillis()))
                        .apiCallAttemptTimeout(java.time.Duration.ofMillis(connectTimeout.toMillis()))
                        .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgentPrefix)
                        .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX, s3UserAgentSuffix))
                .httpClientBuilder(ApacheHttpClient.builder()
                        .maxConnections(maxConnections)
                        .connectionTimeout(java.time.Duration.ofMillis(connectTimeout.toMillis()))
                        .socketTimeout(java.time.Duration.ofMillis(socketTimeout.toMillis())));

        configureRegionAndEndpoint(clientBuilder, config, defaults);
        return clientBuilder.build();
    }

    private AwsCredentialsProvider getAwsCredentialsProvider(Configuration conf, HiveS3Config defaults)
    {
        Optional<AwsCredentials> credentials = getAwsCredentials(conf);
        if (credentials.isPresent()) {
            return StaticCredentialsProvider.create(credentials.get());
        }

        boolean useInstanceCredentials = conf.getBoolean(S3_USE_INSTANCE_CREDENTIALS, defaults.isS3UseInstanceCredentials());
        if (useInstanceCredentials) {
            return InstanceProfileCredentialsProvider.create();
        }

        String providerClass = conf.get(S3_CREDENTIALS_PROVIDER);
        if (!isNullOrEmpty(providerClass)) {
            return getCustomAWSCredentialsProvider(conf, providerClass);
        }

        return DefaultCredentialsProvider.create();
    }

    private static AwsCredentialsProvider getCustomAWSCredentialsProvider(Configuration conf, String providerClass)
    {
        try {
            return conf.getClassByName(providerClass)
                    .asSubclass(AwsCredentialsProvider.class)
                    .getConstructor(URI.class, Configuration.class)
                    .newInstance(null, conf);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(format("Error creating an instance of %s", providerClass), e);
        }
    }

    private static Optional<AwsCredentials> getAwsCredentials(Configuration conf)
    {
        String accessKey = conf.get(S3_ACCESS_KEY);
        String secretKey = conf.get(S3_SECRET_KEY);

        if (isNullOrEmpty(accessKey) || isNullOrEmpty(secretKey)) {
            return Optional.empty();
        }
        return Optional.of(AwsBasicCredentials.create(accessKey, secretKey));
    }

    private S3AsyncClient buildS3AsyncClient(Configuration config, HiveClientConfig clientConfig)
    {
        HiveS3Config defaults = new HiveS3Config();
        String userAgentPrefix = config.get(S3_USER_AGENT_PREFIX, defaults.getS3UserAgentPrefix());
        int maxErrorRetries = config.getInt(S3_MAX_ERROR_RETRIES, defaults.getS3MaxErrorRetries());
        boolean sslEnabled = config.getBoolean(S3_SSL_ENABLED, defaults.isS3SslEnabled());
        Duration connectTimeout = Duration.valueOf(config.get(S3_CONNECT_TIMEOUT, defaults.getS3ConnectTimeout().toString()));
        Duration socketTimeout = Duration.valueOf(config.get(S3_SOCKET_TIMEOUT, defaults.getS3SocketTimeout().toString()));
        int maxConnections = config.getInt(S3_SELECT_PUSHDOWN_MAX_CONNECTIONS, clientConfig.getS3SelectPushdownMaxConnections());

        if (clientConfig.isS3SelectPushdownEnabled()) {
            s3UserAgentSuffix = "presto-select";
        }

        AwsCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(config, defaults);

        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(builder -> builder
                        .retryStrategy(retryStrategy -> retryStrategy.maxAttempts(maxErrorRetries))
                        .apiCallTimeout(java.time.Duration.ofMillis(socketTimeout.toMillis()))
                        .apiCallAttemptTimeout(java.time.Duration.ofMillis(connectTimeout.toMillis()))
                        .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgentPrefix)
                        .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX, s3UserAgentSuffix))
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .maxConcurrency(maxConnections)
                        .connectionTimeout(java.time.Duration.ofMillis(connectTimeout.toMillis()))
                        .readTimeout(java.time.Duration.ofMillis(socketTimeout.toMillis()))
                        .writeTimeout(java.time.Duration.ofMillis(socketTimeout.toMillis())))
                .forcePathStyle(true);

        configureRegionAndEndpoint(clientBuilder, config, defaults);
        return clientBuilder.build();
    }

    private void configureRegionAndEndpoint(S3ClientBuilder clientBuilder, Configuration config, HiveS3Config defaults)
    {
        boolean regionOrEndpointSet = false;

        String endpoint = config.get(S3_ENDPOINT);
        boolean pinS3ClientToCurrentRegion = config.getBoolean(S3_PIN_CLIENT_TO_CURRENT_REGION, defaults.isPinS3ClientToCurrentRegion());
        verify(!pinS3ClientToCurrentRegion || endpoint == null,
                "Invalid configuration: either endpoint can be set or S3 client can be pinned to the current region");

        // use local region when running inside of EC2
        if (pinS3ClientToCurrentRegion) {
            try {
                Region region = new DefaultAwsRegionProviderChain().getRegion();
                if (region != null) {
                    clientBuilder.region(region);
                    regionOrEndpointSet = true;
                    log.debug("Using region from provider chain: %s", region);
                }
            }
            catch (Exception e) {
                log.debug("Could not determine current region from provider chain: %s", e.getMessage());
            }
        }

        if (!isNullOrEmpty(endpoint)) {
            clientBuilder.endpointOverride(URI.create(endpoint));

            // Defaulting to the us-east-1 region.
            // In AWS SDK V1, Presto would automatically use us-east-1 if no region was specified.
            // However, AWS SDK V2 determines the region using the DefaultAwsRegionProviderChain,
            // which may not be available when Presto is not running on EC2.
            clientBuilder.region(Region.US_EAST_1);

            log.debug("Using custom endpoint: %s", endpoint);
            regionOrEndpointSet = true;
        }

        clientBuilder.forcePathStyle(true);

        if (!regionOrEndpointSet) {
            clientBuilder.region(Region.US_EAST_1);
            clientBuilder.crossRegionAccessEnabled(true);
            log.debug("No region or endpoint specified, defaulting to US_EAST_1");
        }
    }

    private void configureRegionAndEndpoint(S3AsyncClientBuilder clientBuilder, Configuration config, HiveS3Config defaults)
    {
        boolean regionOrEndpointSet = false;

        String endpoint = config.get(S3_ENDPOINT);
        boolean pinS3ClientToCurrentRegion = config.getBoolean(S3_PIN_CLIENT_TO_CURRENT_REGION, defaults.isPinS3ClientToCurrentRegion());
        verify(!pinS3ClientToCurrentRegion || endpoint == null,
                "Invalid configuration: either endpoint can be set or S3 client can be pinned to the current region");

        // use local region when running inside of EC2
        if (pinS3ClientToCurrentRegion) {
            try {
                Region region = software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain.builder().build().getRegion();
                if (region != null) {
                    clientBuilder.region(region);
                    regionOrEndpointSet = true;
                    log.debug("Using region from provider chain: %s", region);
                }
            }
            catch (Exception e) {
                log.debug("Could not determine current region from provider chain: %s", e.getMessage());
                // Continue to fallback
            }
        }

        if (!isNullOrEmpty(endpoint)) {
            clientBuilder.endpointOverride(URI.create(endpoint));
            log.debug("Using custom endpoint: %s", endpoint);

            // Defaulting to the us-east-1 region.
            // In AWS SDK V1, Presto would automatically use us-east-1 if no region was specified.
            // However, AWS SDK V2 determines the region using the DefaultAwsRegionProviderChain,
            // which may not be available when Presto is not running on EC2.
            clientBuilder.region(Region.US_EAST_1);

            regionOrEndpointSet = true;
        }

        if (!regionOrEndpointSet) {
            clientBuilder.region(Region.US_EAST_1);
            clientBuilder.crossRegionAccessEnabled(true);
            log.debug("No region or endpoint specified, defaulting to US_EAST_1");
        }
    }
}
