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
package com.facebook.presto.hive;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import com.facebook.presto.hive.metastore.AbstractCachingHiveMetastore.MetastoreCacheScope;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.CustomMetastoreAuthConfigUtils.getMetastoreToken;
import static com.facebook.presto.hive.CustomMetastoreAuthConfigUtils.getMetastoreUsername;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;

public class MetastoreClientConfig
{
    private HostAndPort metastoreSocksProxy;
    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean verifyChecksum = true;
    private boolean requireHadoopNative = true;

    private Duration metastoreCacheTtl = new Duration(0, TimeUnit.SECONDS);
    private Duration metastoreRefreshInterval = new Duration(0, TimeUnit.SECONDS);
    private long metastoreCacheMaximumSize = 10000;
    private long perTransactionMetastoreCacheMaximumSize = 1000;
    private int maxMetastoreRefreshThreads = 100;

    private String recordingPath;
    private boolean replay;
    private Duration recordingDuration = new Duration(0, MINUTES);
    private boolean partitionVersioningEnabled;
    private MetastoreCacheScope metastoreCacheScope = MetastoreCacheScope.ALL;
    private boolean metastoreImpersonationEnabled;
    private double partitionCacheValidationPercentage;
    private int partitionCacheColumnCountLimit = 500;
    private HiveMetastoreAuthenticationType hiveMetastoreAuthenticationType = HiveMetastoreAuthenticationType.NONE;
    private HttpHiveMetastoreClientAuthenticationType httpHiveMetastoreClientAuthenticationType = HttpHiveMetastoreClientAuthenticationType.NONE;

    private boolean deleteFilesOnTableDrop;
    private boolean invalidateMetastoreCacheProcedureEnabled;
    private Duration httpReadTimeout = new Duration(60, TimeUnit.SECONDS);
    private Map<String, String> httpAdditionalHeaders = ImmutableMap.of();
    private boolean httpMetastoreTlsEnabled;
    private File httpMetastoreTlsKeystorePath;
    private String httpMetastoreTlsKeystorePassword;
    private File httpMetastoreTlsTruststorePath;
    private String httpMetastoreTlsTruststorePassword;
    private Optional<String> httpMetastoreBasicUsername = Optional.empty();
    private Optional<String> httpMetastoreBasicPassword = Optional.empty();
    private Optional<String> httpBearerToken = Optional.empty();

    public HostAndPort getMetastoreSocksProxy()
    {
        return metastoreSocksProxy;
    }

    @Config("hive.metastore.thrift.client.socks-proxy")
    public MetastoreClientConfig setMetastoreSocksProxy(HostAndPort metastoreSocksProxy)
    {
        this.metastoreSocksProxy = metastoreSocksProxy;
        return this;
    }

    @NotNull
    public Duration getMetastoreTimeout()
    {
        return metastoreTimeout;
    }

    @Config("hive.metastore-timeout")
    public MetastoreClientConfig setMetastoreTimeout(Duration metastoreTimeout)
    {
        this.metastoreTimeout = metastoreTimeout;
        return this;
    }

    public boolean isVerifyChecksum()
    {
        return verifyChecksum;
    }

    @Config("hive.dfs.verify-checksum")
    public MetastoreClientConfig setVerifyChecksum(boolean verifyChecksum)
    {
        this.verifyChecksum = verifyChecksum;
        return this;
    }

    @NotNull
    public Duration getMetastoreCacheTtl()
    {
        return metastoreCacheTtl;
    }

    @MinDuration("0ms")
    @Config("hive.metastore-cache-ttl")
    public MetastoreClientConfig setMetastoreCacheTtl(Duration metastoreCacheTtl)
    {
        this.metastoreCacheTtl = metastoreCacheTtl;
        return this;
    }

    @NotNull
    public Duration getMetastoreRefreshInterval()
    {
        return metastoreRefreshInterval;
    }

    @MinDuration("1ms")
    @Config("hive.metastore-refresh-interval")
    public MetastoreClientConfig setMetastoreRefreshInterval(Duration metastoreRefreshInterval)
    {
        this.metastoreRefreshInterval = metastoreRefreshInterval;
        return this;
    }

    public long getMetastoreCacheMaximumSize()
    {
        return metastoreCacheMaximumSize;
    }

    @Min(1)
    @Config("hive.metastore-cache-maximum-size")
    public MetastoreClientConfig setMetastoreCacheMaximumSize(long metastoreCacheMaximumSize)
    {
        this.metastoreCacheMaximumSize = metastoreCacheMaximumSize;
        return this;
    }

    public long getPerTransactionMetastoreCacheMaximumSize()
    {
        return perTransactionMetastoreCacheMaximumSize;
    }

    @Min(1)
    @Config("hive.per-transaction-metastore-cache-maximum-size")
    public MetastoreClientConfig setPerTransactionMetastoreCacheMaximumSize(long perTransactionMetastoreCacheMaximumSize)
    {
        this.perTransactionMetastoreCacheMaximumSize = perTransactionMetastoreCacheMaximumSize;
        return this;
    }

    @Min(1)
    public int getMaxMetastoreRefreshThreads()
    {
        return maxMetastoreRefreshThreads;
    }

    @Config("hive.metastore-refresh-max-threads")
    public MetastoreClientConfig setMaxMetastoreRefreshThreads(int maxMetastoreRefreshThreads)
    {
        this.maxMetastoreRefreshThreads = maxMetastoreRefreshThreads;
        return this;
    }

    public String getRecordingPath()
    {
        return recordingPath;
    }

    @Config("hive.metastore-recording-path")
    public MetastoreClientConfig setRecordingPath(String recordingPath)
    {
        this.recordingPath = recordingPath;
        return this;
    }

    public boolean isReplay()
    {
        return replay;
    }

    @Config("hive.replay-metastore-recording")
    public MetastoreClientConfig setReplay(boolean replay)
    {
        this.replay = replay;
        return this;
    }

    @NotNull
    public Duration getRecordingDuration()
    {
        return recordingDuration;
    }

    @Config("hive.metastore-recoding-duration")
    public MetastoreClientConfig setRecordingDuration(Duration recordingDuration)
    {
        this.recordingDuration = recordingDuration;
        return this;
    }

    public boolean isRequireHadoopNative()
    {
        return requireHadoopNative;
    }

    @Config("hive.dfs.require-hadoop-native")
    public MetastoreClientConfig setRequireHadoopNative(boolean requireHadoopNative)
    {
        this.requireHadoopNative = requireHadoopNative;
        return this;
    }

    public boolean isPartitionVersioningEnabled()
    {
        return partitionVersioningEnabled;
    }

    @Config("hive.partition-versioning-enabled")
    public MetastoreClientConfig setPartitionVersioningEnabled(boolean partitionVersioningEnabled)
    {
        this.partitionVersioningEnabled = partitionVersioningEnabled;
        return this;
    }

    @NotNull
    public MetastoreCacheScope getMetastoreCacheScope()
    {
        return metastoreCacheScope;
    }

    @Config("hive.metastore-cache-scope")
    public MetastoreClientConfig setMetastoreCacheScope(MetastoreCacheScope metastoreCacheScope)
    {
        this.metastoreCacheScope = metastoreCacheScope;
        return this;
    }

    public boolean isMetastoreImpersonationEnabled()
    {
        return metastoreImpersonationEnabled;
    }

    @Config("hive.metastore-impersonation-enabled")
    @ConfigDescription("Should Presto user be impersonated when communicating with Hive Metastore")
    public MetastoreClientConfig setMetastoreImpersonationEnabled(boolean metastoreImpersonationEnabled)
    {
        this.metastoreImpersonationEnabled = metastoreImpersonationEnabled;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("100.0")
    public double getPartitionCacheValidationPercentage()
    {
        return partitionCacheValidationPercentage;
    }

    @Config("hive.partition-cache-validation-percentage")
    public MetastoreClientConfig setPartitionCacheValidationPercentage(double partitionCacheValidationPercentage)
    {
        this.partitionCacheValidationPercentage = partitionCacheValidationPercentage;
        return this;
    }

    public int getPartitionCacheColumnCountLimit()
    {
        return partitionCacheColumnCountLimit;
    }

    @Config("hive.partition-cache-column-count-limit")
    @ConfigDescription("The max limit on the column count for a partition to be cached")
    public MetastoreClientConfig setPartitionCacheColumnCountLimit(int partitionCacheColumnCountLimit)
    {
        this.partitionCacheColumnCountLimit = partitionCacheColumnCountLimit;
        return this;
    }

    public enum HiveMetastoreAuthenticationType
    {
        NONE,
        KERBEROS
    }

    public enum HttpHiveMetastoreClientAuthenticationType
    {
        NONE,
        BASIC,
        BEARER
    }

    @NotNull
    public HiveMetastoreAuthenticationType getHiveMetastoreAuthenticationType()
    {
        return hiveMetastoreAuthenticationType;
    }

    @Config("hive.metastore.authentication.type")
    @ConfigDescription("Hive Metastore authentication type")
    public MetastoreClientConfig setHiveMetastoreAuthenticationType(HiveMetastoreAuthenticationType hiveMetastoreAuthenticationType)
    {
        this.hiveMetastoreAuthenticationType = hiveMetastoreAuthenticationType;
        return this;
    }

    public boolean isDeleteFilesOnTableDrop()
    {
        return deleteFilesOnTableDrop;
    }

    @Config("hive.metastore.thrift.delete-files-on-table-drop")
    @ConfigDescription("Delete files on dropping table in case the metastore fails to do so")
    public MetastoreClientConfig setDeleteFilesOnTableDrop(boolean deleteFilesOnTableDrop)
    {
        this.deleteFilesOnTableDrop = deleteFilesOnTableDrop;
        return this;
    }

    public boolean isInvalidateMetastoreCacheProcedureEnabled()
    {
        return invalidateMetastoreCacheProcedureEnabled;
    }

    @Config("hive.invalidate-metastore-cache-procedure-enabled")
    @ConfigDescription("When enabled, users will be able to invalidate metastore cache on demand")
    public MetastoreClientConfig setInvalidateMetastoreCacheProcedureEnabled(boolean invalidateMetastoreCacheProcedureEnabled)
    {
        this.invalidateMetastoreCacheProcedureEnabled = invalidateMetastoreCacheProcedureEnabled;
        return this;
    }

    public Optional<String> getHttpBearerToken()
    {
        return httpBearerToken;
    }

    @Config("hive.metastore.http.client.bearer-token")
    @ConfigSecuritySensitive
    @ConfigDescription("Bearer token to authenticate with a HTTP transport based metastore service")
    public MetastoreClientConfig setHttpBearerToken(String httpBearerToken)
    {
        this.httpBearerToken = Optional.ofNullable(httpBearerToken);
        return this;
    }

    @NotNull
    public Duration getHttpReadTimeout()
    {
        return httpReadTimeout;
    }

    @Config("hive.metastore.http.client.read-timeout")
    @ConfigDescription("Socket read timeout for metastore client")
    public MetastoreClientConfig setHttpReadTimeout(Duration readTimeout)
    {
        this.httpReadTimeout = readTimeout;
        return this;
    }

    public Map<String, String> getHttpAdditionalHeaders()
    {
        return httpAdditionalHeaders;
    }

    @Config("hive.metastore.http.client.additional-headers")
    @ConfigDescription("Comma separated key:value pairs to be send to metastore as additional headers")
    public MetastoreClientConfig setHttpAdditionalHeaders(String httpHeaders)
    {
        try {
            // we allow escaping the delimiters like , and : using back-slash.
            // To support that we create a negative lookbehind of , and : which
            // are not preceded by a back-slash.
            String headersDelim = "(?<!\\\\),";
            String kvDelim = "(?<!\\\\):";
            Map<String, String> temp = new HashMap<>();
            if (httpHeaders != null) {
                for (String kv : httpHeaders.split(headersDelim)) {
                    String key = kv.split(kvDelim, 2)[0].trim();
                    String val = kv.split(kvDelim, 2)[1].trim();
                    temp.put(key, val);
                }
                this.httpAdditionalHeaders = ImmutableMap.copyOf(temp);
            }
        }
        catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(format("Invalid format for 'hive.metastore.http.client.additional-headers'. " +
                    "Value provided is %s", httpHeaders), e);
        }
        return this;
    }

    @Config("hive.metastore.http.client.tls.enabled")
    @ConfigDescription("Enable TLS security for HMS")
    public MetastoreClientConfig setHttpMetastoreTlsEnabled(boolean enabled)
    {
        this.httpMetastoreTlsEnabled = enabled;
        return this;
    }

    public boolean getHttpMetastoreTlsEnabled()
    {
        return httpMetastoreTlsEnabled;
    }

    @Config("hive.metastore.http.client.tls.keystore-path")
    @ConfigDescription("Path to JKS or PEM key store")
    public MetastoreClientConfig setHttpMetastoreTlsKeystorePath(File keystorePath)
    {
        this.httpMetastoreTlsKeystorePath = keystorePath;
        return this;
    }

    public File getHttpMetastoreTlsKeystorePath()
    {
        return httpMetastoreTlsKeystorePath;
    }

    @Config("hive.metastore.http.client.tls.keystore-password")
    @ConfigDescription("Password to key store")
    @ConfigSecuritySensitive
    public MetastoreClientConfig setHttpMetastoreTlsKeystorePassword(String keystorePassword)
    {
        this.httpMetastoreTlsKeystorePassword = keystorePassword;
        return this;
    }

    public String getHttpMetastoreTlsKeystorePassword()
    {
        return httpMetastoreTlsKeystorePassword;
    }

    @Config("hive.metastore.http.client.tls.truststore-path")
    @ConfigDescription("Path to JKS or PEM trust store")
    public MetastoreClientConfig setHttpMetastoreTlsTruststorePath(File truststorePath)
    {
        this.httpMetastoreTlsTruststorePath = truststorePath;
        return this;
    }

    public File getHttpMetastoreTlsTruststorePath()
    {
        return httpMetastoreTlsTruststorePath;
    }

    @Config("hive.metastore.http.client.tls.truststore-password")
    @ConfigDescription("Path to trust store")
    public MetastoreClientConfig setHttpMetastoreTlsTruststorePassword(String truststorePassword)
    {
        this.httpMetastoreTlsTruststorePassword = truststorePassword;
        return this;
    }

    public String getHttpMetastoreTlsTruststorePassword()
    {
        return httpMetastoreTlsTruststorePassword;
    }

    @Config("hive.metastore.http.client.auth.basic.password")
    @ConfigDescription("Hive metastore http client authentication basic password")
    public MetastoreClientConfig setHttpMetastoreBasicPassword(String httpMetastoreBasicPassword)
    {
        this.httpMetastoreBasicPassword = Optional.ofNullable(httpMetastoreBasicPassword);
        return this;
    }

    public Optional<String> getHttpMetastoreBasicPassword()
    {
        if (this.httpMetastoreBasicPassword.isPresent()) {
            return this.httpMetastoreBasicPassword;
        }
        return Optional.ofNullable(getMetastoreToken());
    }

    @Config("hive.metastore.http.client.auth.basic.username")
    @ConfigDescription("Hive metastore http client authentication basic username")
    public MetastoreClientConfig setHttpMetastoreBasicUsername(String httpMetastoreBasicUsername)
    {
        this.httpMetastoreBasicUsername = Optional.ofNullable(httpMetastoreBasicUsername);
        return this;
    }

    public Optional<String> getHttpMetastoreBasicUsername()
    {
        if (this.httpMetastoreBasicUsername.isPresent()) {
            return this.httpMetastoreBasicUsername;
        }
        return Optional.ofNullable(getMetastoreUsername());
    }

    @Config("hive.metastore.http.client.authentication.type")
    @ConfigDescription("Hive metastore http client authentication type")
    public MetastoreClientConfig setHttpHiveMetastoreClientAuthenticationType(HttpHiveMetastoreClientAuthenticationType httpHiveMetastoreClientAuthenticationType)
    {
        this.httpHiveMetastoreClientAuthenticationType = httpHiveMetastoreClientAuthenticationType;
        return this;
    }

    @NotNull
    public HttpHiveMetastoreClientAuthenticationType getHttpHiveMetastoreClientAuthenticationType()
    {
        return httpHiveMetastoreClientAuthenticationType;
    }
}
