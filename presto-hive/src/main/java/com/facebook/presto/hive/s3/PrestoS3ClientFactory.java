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
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ENDPOINT;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.time.Duration.ofMillis;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

/**
 * This factory provides S3AsyncClient required for executing S3SelectPushdown requests.
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
    private S3AsyncClient s3AsyncClient;

    synchronized S3AsyncClient getS3AsyncClient(Configuration config, HiveClientConfig clientConfig)
    {
        if (s3AsyncClient != null) {
            return s3AsyncClient;
        }

        s3AsyncClient = buildS3AsyncClient(config, clientConfig);
        return s3AsyncClient;
    }

    private S3AsyncClient buildS3AsyncClient(Configuration config, HiveClientConfig clientConfig)
    {
        HiveS3Config defaults = new HiveS3Config();
        String userAgentPrefix = config.get(S3_USER_AGENT_PREFIX, defaults.getS3UserAgentPrefix());
        int maxErrorRetries = config.getInt(S3_MAX_ERROR_RETRIES, defaults.getS3MaxErrorRetries());
        Duration connectTimeout = Duration.valueOf(config.get(S3_CONNECT_TIMEOUT, defaults.getS3ConnectTimeout().toString()));
        Duration socketTimeout = Duration.valueOf(config.get(S3_SOCKET_TIMEOUT, defaults.getS3SocketTimeout().toString()));
        int maxConnections = config.getInt(S3_SELECT_PUSHDOWN_MAX_CONNECTIONS, clientConfig.getS3SelectPushdownMaxConnections());

        if (clientConfig.isS3SelectPushdownEnabled()) {
            s3UserAgentSuffix = "presto-select";
        }

        AwsCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(config, defaults);

        StandardRetryStrategy strategy = AwsRetryStrategy.standardRetryStrategy()
                .toBuilder()
                .maxAttempts(maxErrorRetries)
                .build();

        ClientOverrideConfiguration clientOverrideConfiguration = ClientOverrideConfiguration.builder()
                .retryStrategy(strategy)
                .putAdvancedOption(USER_AGENT_PREFIX, userAgentPrefix)
                .putAdvancedOption(USER_AGENT_SUFFIX, s3UserAgentSuffix)
                .build();

        boolean sslEnabled = config.getBoolean(S3_SSL_ENABLED, defaults.isS3SslEnabled());
        if (!sslEnabled) {
            log.warn("SSL is disabled - this is not recommended for production use");
        }

        String endpoint = config.get(S3_ENDPOINT);
        boolean isHttpEndpoint = false;
        URI endpointUri = null;

        if (endpoint != null) {
            try {
                endpointUri = URI.create(endpoint);
                if (endpointUri.getScheme() == null) {
                    endpoint = (sslEnabled ? "https://" : "http://") + endpoint;
                    endpointUri = URI.create(endpoint);
                }
                isHttpEndpoint = "http".equalsIgnoreCase(endpointUri.getScheme());
                if (isHttpEndpoint) {
                    log.debug("HTTP endpoint detected: %s - will disable checksum validation", endpoint);
                }
            }
            catch (IllegalArgumentException e) {
                log.error("Invalid S3 endpoint URL: %s", endpoint);
                throw new RuntimeException("Invalid S3 endpoint configuration", e);
            }
        }

        final boolean disableChecksums = isHttpEndpoint;
        S3Configuration s3Configuration = S3Configuration.builder()
                .checksumValidationEnabled(!disableChecksums)
                .build();

        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(clientOverrideConfiguration)
                .serviceConfiguration(s3Configuration)
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .maxConcurrency(maxConnections)
                        .connectionTimeout(ofMillis(connectTimeout.toMillis()))
                        .readTimeout(ofMillis(socketTimeout.toMillis()))
                        .writeTimeout(ofMillis(socketTimeout.toMillis())))
                .forcePathStyle(true);

        configureRegionAndEndpoint(clientBuilder, endpointUri);
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

    private void configureRegionAndEndpoint(S3AsyncClientBuilder clientBuilder, URI endpointUri)
    {
        boolean regionOrEndpointSet = false;

        if (endpointUri != null) {
            clientBuilder.endpointOverride(endpointUri);

            // Defaulting to the us-east-1 region.
            // In AWS SDK V1, Presto would automatically use us-east-1 if no region was specified.
            // However, AWS SDK V2 determines the region using the DefaultAwsRegionProviderChain,
            // which may not be available when Presto is not running on EC2.
            clientBuilder.region(Region.US_EAST_1);

            log.debug("Using custom endpoint: %s", endpointUri);
            regionOrEndpointSet = true;
        }

        if (!regionOrEndpointSet) {
            clientBuilder.region(Region.US_EAST_1);
            clientBuilder.crossRegionAccessEnabled(true);
            log.debug("No region or endpoint specified, defaulting to US_EAST_1");
        }
    }
}
