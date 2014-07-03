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

import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class HiveClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private TimeZone timeZone = TimeZone.getDefault();

    private DataSize maxSplitSize = new DataSize(64, MEGABYTE);
    private int maxOutstandingSplits = 1_000;
    private int maxGlobalSplitIteratorThreads = 1_000;
    private int maxSplitIteratorThreads = 50;
    private int minPartitionBatchSize = 10;
    private int maxPartitionBatchSize = 100;
    private int maxInitialSplits = 200;
    private DataSize maxInitialSplitSize;
    private boolean allowDropTable;

    private Duration metastoreCacheTtl = new Duration(1, TimeUnit.HOURS);
    private Duration metastoreRefreshInterval = new Duration(2, TimeUnit.MINUTES);
    private Duration metastoreCacheGetAllDatabasesTtl = null;
    private Duration metastoreCacheGetAllDatabasesRefreshInterval = null;
    private Duration metastoreCacheGetDatabaseTtl = null;
    private Duration metastoreCacheGetDatabaseRefreshInterval = null;
    private Duration metastoreCacheGetAllTablesTtl = null;
    private Duration metastoreCacheGetAllTablesRefreshInterval = null;
    private Duration metastoreCacheGetTableTtl = null;
    private Duration metastoreCacheGetTableRefreshInterval = null;
    private Duration metastoreCacheGetPartitionNamesTtl = null;
    private Duration metastoreCacheGetPartitionNamesRefreshInterval = null;
    private Duration metastoreCacheGetPartitionNamesByPartsTtl = null;
    private Duration metastoreCacheGetPartitionNamesByPartsRefreshInterval = null;
    private Duration metastoreCacheGetPartitionByNameTtl = null;
    private Duration metastoreCacheGetPartitionByNameRefreshInterval = null;
    private Duration metastoreCacheLoadViewsTtl = null;
    private Duration metastoreCacheLoadViewsRefreshInterval = null;

    private int maxMetastoreRefreshThreads = 100;
    private HostAndPort metastoreSocksProxy;
    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);

    private Duration dfsTimeout = new Duration(10, TimeUnit.SECONDS);
    private Duration dfsConnectTimeout = new Duration(500, TimeUnit.MILLISECONDS);
    private int dfsConnectMaxRetries = 5;

    private String domainSocketPath;

    private String s3AwsAccessKey;
    private String s3AwsSecretKey;
    private boolean s3SslEnabled = true;
    private int s3MaxClientRetries = 3;
    private int s3MaxErrorRetries = 10;
    private Duration s3ConnectTimeout = new Duration(5, TimeUnit.SECONDS);
    private File s3StagingDirectory = new File(StandardSystemProperty.JAVA_IO_TMPDIR.value());

    private List<String> resourceConfigFiles;

    public int getMaxInitialSplits()
    {
        return maxInitialSplits;
    }

    @Config("hive.max-initial-splits")
    public HiveClientConfig setMaxInitialSplits(int maxInitialSplits)
    {
        this.maxInitialSplits = maxInitialSplits;
        return this;
    }

    public DataSize getMaxInitialSplitSize()
    {
        if (maxInitialSplitSize == null) {
            return new DataSize(maxSplitSize.getValue() / 2, maxSplitSize.getUnit());
        }
        return maxInitialSplitSize;
    }

    @Config("hive.max-initial-split-size")
    public HiveClientConfig setMaxInitialSplitSize(DataSize maxInitialSplitSize)
    {
        this.maxInitialSplitSize = maxInitialSplitSize;
        return this;
    }

    @NotNull
    public TimeZone getTimeZone()
    {
        return timeZone;
    }

    @Config("hive.time-zone")
    public HiveClientConfig setTimeZone(String id)
    {
        this.timeZone = (id == null) ? TimeZone.getDefault() : TimeZone.getTimeZone(id);
        return this;
    }

    public HiveClientConfig setTimeZone(TimeZone timeZone)
    {
        this.timeZone = (timeZone == null) ? TimeZone.getDefault() : timeZone;
        return this;
    }

    @NotNull
    public DataSize getMaxSplitSize()
    {
        return maxSplitSize;
    }

    @Config("hive.max-split-size")
    public HiveClientConfig setMaxSplitSize(DataSize maxSplitSize)
    {
        this.maxSplitSize = maxSplitSize;
        return this;
    }

    @Min(1)
    public int getMaxOutstandingSplits()
    {
        return maxOutstandingSplits;
    }

    @Config("hive.max-outstanding-splits")
    public HiveClientConfig setMaxOutstandingSplits(int maxOutstandingSplits)
    {
        this.maxOutstandingSplits = maxOutstandingSplits;
        return this;
    }

    @Min(1)
    public int getMaxSplitIteratorThreads()
    {
        return maxSplitIteratorThreads;
    }

    @Config("hive.max-split-iterator-threads")
    public HiveClientConfig setMaxSplitIteratorThreads(int maxSplitIteratorThreads)
    {
        this.maxSplitIteratorThreads = maxSplitIteratorThreads;
        return this;
    }

    @Min(1)
    public int getMaxGlobalSplitIteratorThreads()
    {
        return maxGlobalSplitIteratorThreads;
    }

    @Config("hive.max-global-split-iterator-threads")
    public HiveClientConfig setMaxGlobalSplitIteratorThreads(int maxGlobalSplitIteratorThreads)
    {
        this.maxGlobalSplitIteratorThreads = maxGlobalSplitIteratorThreads;
        return this;
    }

    public boolean getAllowDropTable()
    {
        return this.allowDropTable;
    }

    @Config("hive.allow-drop-table")
    @ConfigDescription("Allow hive connector to drop table")
    public HiveClientConfig setAllowDropTable(boolean allowDropTable)
    {
        this.allowDropTable = allowDropTable;
        return this;
    }

    @NotNull
    public Duration getMetastoreCacheTtl()
    {
        return metastoreCacheTtl;
    }

    @Config("hive.metastore-cache-ttl")
    public HiveClientConfig setMetastoreCacheTtl(Duration metastoreCacheTtl)
    {
        this.metastoreCacheTtl = metastoreCacheTtl;
        return this;
    }

    @NotNull
    public Duration getMetastoreRefreshInterval()
    {
        return metastoreRefreshInterval;
    }

    @Config("hive.metastore-refresh-interval")
    public HiveClientConfig setMetastoreRefreshInterval(Duration metastoreRefreshInterval)
    {
        this.metastoreRefreshInterval = metastoreRefreshInterval;
        return this;
    }

    public Duration getMetastoreCacheGetAllDatabasesTtl()
    {
        return metastoreCacheGetAllDatabasesTtl;
    }

    @Config("hive.metastore-getalldatabases-cache-ttl")
    public HiveClientConfig setMetastoreCacheGetAllDatabasesTtl(Duration metastoreCacheGetAllDatabasesTtl)
    {
        this.metastoreCacheGetAllDatabasesTtl = metastoreCacheGetAllDatabasesTtl;
        return this;
    }

    public Duration getMetastoreCacheGetAllDatabasesRefreshInterval()
    {
        return metastoreCacheGetAllDatabasesRefreshInterval;
    }

    @Config("hive.metastore-getalldatabases-refresh-interval")
    public HiveClientConfig setMetastoreCacheGetAllDatabasesRefreshInterval(Duration metastoreCacheGetAllDatabasesRefreshInterval)
    {
        this.metastoreCacheGetAllDatabasesRefreshInterval = metastoreCacheGetAllDatabasesRefreshInterval;
        return this;
    }

    public Duration getMetastoreCacheGetDatabaseTtl()
    {
        return metastoreCacheGetDatabaseTtl;
    }

    @Config("hive.metastore-getdatabase-cache-ttl")
    public HiveClientConfig setMetastoreCacheGetDatabaseTtl(Duration metastoreCacheGetDatabaseTtl)
    {
        this.metastoreCacheGetDatabaseTtl = metastoreCacheGetDatabaseTtl;
        return this;
    }

    public Duration getMetastoreCacheGetDatabaseRefreshInterval()
    {
        return metastoreCacheGetDatabaseRefreshInterval;
    }

    @Config("hive.metastore-getdatabase-refresh-interval")
    public HiveClientConfig setMetastoreCacheGetDatabaseRefreshInterval(Duration metastoreCacheGetDatabaseRefreshInterval)
    {
        this.metastoreCacheGetDatabaseRefreshInterval = metastoreCacheGetDatabaseRefreshInterval;
        return this;
    }

    public Duration getMetastoreCacheGetAllTablesTtl()
    {
        return metastoreCacheGetAllTablesTtl;
    }

    @Config("hive.metastore-getalltables-cache-ttl")
    public HiveClientConfig setMetastoreCacheGetAllTablesTtl(Duration metastoreCacheGetAllTablesTtl)
    {
        this.metastoreCacheGetAllTablesTtl = metastoreCacheGetAllTablesTtl;
        return this;
    }

    public Duration getMetastoreCacheGetAllTablesRefreshInterval()
    {
        return metastoreCacheGetAllTablesRefreshInterval;
    }

    @Config("hive.metastore-getalltables-refresh-interval")
    public HiveClientConfig setMetastoreCacheGetAllTablesRefreshInterval(Duration metastoreCacheGetAllTablesRefreshInterval)
    {
        this.metastoreCacheGetAllTablesRefreshInterval = metastoreCacheGetAllTablesRefreshInterval;
        return this;
    }

    public Duration getMetastoreCacheGetTableTtl()
    {
        return metastoreCacheGetTableTtl;
    }

    @Config("hive.metastore-gettable-cache-ttl")
    public HiveClientConfig setMetastoreCacheGetTableTtl(Duration metastoreCacheGetTableTtl)
    {
        this.metastoreCacheGetTableTtl = metastoreCacheGetTableTtl;
        return this;
    }

    public Duration getMetastoreCacheGetTableRefreshInterval()
    {
        return metastoreCacheGetTableRefreshInterval;
    }

    @Config("hive.metastore-gettable-refresh-interval")
    public HiveClientConfig setMetastoreCacheGetTableRefreshInterval(Duration metastoreCacheGetTableRefreshInterval)
    {
        this.metastoreCacheGetTableRefreshInterval = metastoreCacheGetTableRefreshInterval;
        return this;
    }

    public Duration getMetastoreCacheGetPartitionNamesTtl()
    {
        return metastoreCacheGetPartitionNamesTtl;
    }

    @Config("hive.metastore-getpartitionnames-cache-ttl")
    public HiveClientConfig setMetastoreCacheGetPartitionNamesTtl(Duration metastoreCacheGetPartitionNamesTtl)
    {
        this.metastoreCacheGetPartitionNamesTtl = metastoreCacheGetPartitionNamesTtl;
        return this;
    }

    public Duration getMetastoreCacheGetPartitionNamesRefreshInterval()
    {
        return metastoreCacheGetPartitionNamesRefreshInterval;
    }

    @Config("hive.metastore-getpartitionnames-refresh-interval")
    public HiveClientConfig setMetastoreCacheGetPartitionNamesRefreshInterval(Duration metastoreCacheGetPartitionNamesRefreshInterval)
    {
        this.metastoreCacheGetPartitionNamesRefreshInterval = metastoreCacheGetPartitionNamesRefreshInterval;
        return this;
    }

    public Duration getMetastoreCacheGetPartitionNamesByPartsTtl()
    {
        return metastoreCacheGetPartitionNamesByPartsTtl;
    }

    @Config("hive.metastore-getpartitionnamesbyparts-cache-ttl")
    public HiveClientConfig setMetastoreCacheGetPartitionNamesByPartsTtl(Duration metastoreCacheGetPartitionNamesByPartsTtl)
    {
        this.metastoreCacheGetPartitionNamesByPartsTtl = metastoreCacheGetPartitionNamesByPartsTtl;
        return this;
    }

    public Duration getMetastoreCacheGetPartitionNamesByPartsRefreshInterval()
    {
        return metastoreCacheGetPartitionNamesByPartsRefreshInterval;
    }

    @Config("hive.metastore-getpartitionnamesbyparts-refresh-interval")
    public HiveClientConfig setMetastoreCacheGetPartitionNamesByPartsRefreshInterval(Duration metastoreCacheGetPartitionNamesByPartsRefreshInterval)
    {
        this.metastoreCacheGetPartitionNamesByPartsRefreshInterval = metastoreCacheGetPartitionNamesByPartsRefreshInterval;
        return this;
    }

    public Duration getMetastoreCacheGetPartitionByNameTtl()
    {
        return metastoreCacheGetPartitionByNameTtl;
    }

    @Config("hive.metastore-getpartitionbyname-cache-ttl")
    public HiveClientConfig setMetastoreCacheGetPartitionByNameTtl(Duration metastoreCacheGetPartitionByNameTtl)
    {
        this.metastoreCacheGetPartitionByNameTtl = metastoreCacheGetPartitionByNameTtl;
        return this;
    }

    public Duration getMetastoreCacheGetPartitionByNameRefreshInterval()
    {
        return metastoreCacheGetPartitionByNameRefreshInterval;
    }

    @Config("hive.metastore-getpartitionbyname-refresh-interval")
    public HiveClientConfig setMetastoreCacheGetPartitionByNameRefreshInterval(Duration metastoreCacheGetPartitionByNameRefreshInterval)
    {
        this.metastoreCacheGetPartitionByNameRefreshInterval = metastoreCacheGetPartitionByNameRefreshInterval;
        return this;
    }

    public Duration getMetastoreCacheLoadViewsRefreshInterval()
    {
        return metastoreCacheLoadViewsRefreshInterval;
    }

    @Config("hive.metastore-loadviews-refresh-interval")
    public HiveClientConfig setMetastoreCacheLoadViewsRefreshInterval(Duration metastoreCacheLoadViewsRefreshInterval)
    {
        this.metastoreCacheLoadViewsRefreshInterval = metastoreCacheLoadViewsRefreshInterval;
        return this;
    }

    public Duration getMetastoreCacheLoadViewsTtl()
    {
        return metastoreCacheLoadViewsTtl;
    }

    @Config("hive.metastore-loadviews-cache-ttl")
    public HiveClientConfig setMetastoreCacheLoadViewsTtl(Duration metastoreCacheLoadViewsTtl)
    {
        this.metastoreCacheLoadViewsTtl = metastoreCacheLoadViewsTtl;
        return this;
    }

    @Min(1)
    public int getMaxMetastoreRefreshThreads()
    {
        return maxMetastoreRefreshThreads;
    }

    @Config("hive.metastore-refresh-max-threads")
    public HiveClientConfig setMaxMetastoreRefreshThreads(int maxMetastoreRefreshThreads)
    {
        this.maxMetastoreRefreshThreads = maxMetastoreRefreshThreads;
        return this;
    }

    public HostAndPort getMetastoreSocksProxy()
    {
        return metastoreSocksProxy;
    }

    @Config("hive.metastore.thrift.client.socks-proxy")
    public HiveClientConfig setMetastoreSocksProxy(HostAndPort metastoreSocksProxy)
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
    public HiveClientConfig setMetastoreTimeout(Duration metastoreTimeout)
    {
        this.metastoreTimeout = metastoreTimeout;
        return this;
    }

    @Min(1)
    public int getMinPartitionBatchSize()
    {
        return minPartitionBatchSize;
    }

    @Config("hive.metastore.partition-batch-size.min")
    public HiveClientConfig setMinPartitionBatchSize(int minPartitionBatchSize)
    {
        this.minPartitionBatchSize = minPartitionBatchSize;
        return this;
    }

    @Min(1)
    public int getMaxPartitionBatchSize()
    {
        return maxPartitionBatchSize;
    }

    @Config("hive.metastore.partition-batch-size.max")
    public HiveClientConfig setMaxPartitionBatchSize(int maxPartitionBatchSize)
    {
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        return this;
    }

    public List<String> getResourceConfigFiles()
    {
        return resourceConfigFiles;
    }

    @Config("hive.config.resources")
    public HiveClientConfig setResourceConfigFiles(String files)
    {
        this.resourceConfigFiles = (files == null) ? null : SPLITTER.splitToList(files);
        return this;
    }

    public HiveClientConfig setResourceConfigFiles(List<String> files)
    {
        this.resourceConfigFiles = (files == null) ? null : ImmutableList.copyOf(files);
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getDfsTimeout()
    {
        return dfsTimeout;
    }

    @Config("hive.dfs-timeout")
    public HiveClientConfig setDfsTimeout(Duration dfsTimeout)
    {
        this.dfsTimeout = dfsTimeout;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getDfsConnectTimeout()
    {
        return dfsConnectTimeout;
    }

    @Config("hive.dfs.connect.timeout")
    public HiveClientConfig setDfsConnectTimeout(Duration dfsConnectTimeout)
    {
        this.dfsConnectTimeout = dfsConnectTimeout;
        return this;
    }

    @Min(0)
    public int getDfsConnectMaxRetries()
    {
        return dfsConnectMaxRetries;
    }

    @Config("hive.dfs.connect.max-retries")
    public HiveClientConfig setDfsConnectMaxRetries(int dfsConnectMaxRetries)
    {
        this.dfsConnectMaxRetries = dfsConnectMaxRetries;
        return this;
    }

    public String getDomainSocketPath()
    {
        return domainSocketPath;
    }

    @Config("dfs.domain-socket-path")
    public HiveClientConfig setDomainSocketPath(String domainSocketPath)
    {
        this.domainSocketPath = domainSocketPath;
        return this;
    }

    public String getS3AwsAccessKey()
    {
        return s3AwsAccessKey;
    }

    @Config("hive.s3.aws-access-key")
    public HiveClientConfig setS3AwsAccessKey(String s3AwsAccessKey)
    {
        this.s3AwsAccessKey = s3AwsAccessKey;
        return this;
    }

    public String getS3AwsSecretKey()
    {
        return s3AwsSecretKey;
    }

    @Config("hive.s3.aws-secret-key")
    public HiveClientConfig setS3AwsSecretKey(String s3AwsSecretKey)
    {
        this.s3AwsSecretKey = s3AwsSecretKey;
        return this;
    }

    public boolean isS3SslEnabled()
    {
        return s3SslEnabled;
    }

    @Config("hive.s3.ssl.enabled")
    public HiveClientConfig setS3SslEnabled(boolean s3SslEnabled)
    {
        this.s3SslEnabled = s3SslEnabled;
        return this;
    }

    @Min(0)
    public int getS3MaxClientRetries()
    {
        return s3MaxClientRetries;
    }

    @Config("hive.s3.max-client-retries")
    public HiveClientConfig setS3MaxClientRetries(int s3MaxClientRetries)
    {
        this.s3MaxClientRetries = s3MaxClientRetries;
        return this;
    }

    @Min(0)
    public int getS3MaxErrorRetries()
    {
        return s3MaxErrorRetries;
    }

    @Config("hive.s3.max-error-retries")
    public HiveClientConfig setS3MaxErrorRetries(int s3MaxErrorRetries)
    {
        this.s3MaxErrorRetries = s3MaxErrorRetries;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3ConnectTimeout()
    {
        return s3ConnectTimeout;
    }

    @Config("hive.s3.connect-timeout")
    public HiveClientConfig setS3ConnectTimeout(Duration s3ConnectTimeout)
    {
        this.s3ConnectTimeout = s3ConnectTimeout;
        return this;
    }

    @NotNull
    public File getS3StagingDirectory()
    {
        return s3StagingDirectory;
    }

    @Config("hive.s3.staging-directory")
    @ConfigDescription("Temporary directory for staging files before uploading to S3")
    public HiveClientConfig setS3StagingDirectory(File s3StagingDirectory)
    {
        this.s3StagingDirectory = s3StagingDirectory;
        return this;
    }
}
