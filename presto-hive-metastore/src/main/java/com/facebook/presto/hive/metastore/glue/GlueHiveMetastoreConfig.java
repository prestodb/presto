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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import com.facebook.airlift.configuration.DefunctConfig;
import com.facebook.airlift.units.Duration;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@DefunctConfig("hive.metastore.glue.pin-client-to-current-region")
public class GlueHiveMetastoreConfig
{
    private Optional<String> glueRegion = Optional.empty();
    private Optional<String> glueEndpointUrl = Optional.empty();
    private Optional<String> glueStsRegion = Optional.empty();
    private Optional<String> glueStsEndpointUrl = Optional.empty();
    private int maxGlueErrorRetries = 10;
    private int maxGlueConnections = 50;
    private Optional<String> defaultWarehouseDir = Optional.empty();
    private Optional<String> catalogId = Optional.empty();
    private int partitionSegments = 5;
    private int getPartitionThreads = 50;
    private int readStatisticsThreads = 10;
    private int writeStatisticsThreads = 10;
    private Optional<String> iamRole = Optional.empty();
    private boolean columnStatisticsEnabled;
    private Optional<String> awsAccessKey = Optional.empty();
    private Optional<String> awsSecretKey = Optional.empty();
    private int maxUnprocessedKeysRetries = 3;
    private Duration unprocessedKeysRetryMinDelay = new Duration(100, TimeUnit.MILLISECONDS);
    private Duration unprocessedKeysRetryMaxDelay = new Duration(5, TimeUnit.SECONDS);
    private boolean failOnMissingPartitionInStatisticsUpdate = true;

    public Optional<String> getGlueRegion()
    {
        return glueRegion;
    }

    @Config("hive.metastore.glue.region")
    @ConfigDescription("AWS Region for Glue Data Catalog")
    public GlueHiveMetastoreConfig setGlueRegion(String region)
    {
        this.glueRegion = Optional.ofNullable(region);
        return this;
    }

    public Optional<String> getGlueEndpointUrl()
    {
        return glueEndpointUrl;
    }

    @Config("hive.metastore.glue.endpoint-url")
    @ConfigDescription("Glue API endpoint URL")
    public GlueHiveMetastoreConfig setGlueEndpointUrl(String glueEndpointUrl)
    {
        this.glueEndpointUrl = Optional.ofNullable(glueEndpointUrl);
        return this;
    }

    public Optional<String> getGlueStsRegion()
    {
        return glueStsRegion;
    }

    @Config("hive.metastore.glue.sts.region")
    @ConfigDescription("AWS STS region for Glue authentication")
    public GlueHiveMetastoreConfig setGlueStsRegion(String region)
    {
        this.glueStsRegion = Optional.ofNullable(region);
        return this;
    }

    public Optional<String> getGlueStsEndpointUrl()
    {
        return glueStsEndpointUrl;
    }

    @Config("hive.metastore.glue.sts.endpoint-url")
    @ConfigDescription("AWS STS endpoint URL for Glue authentication")
    public GlueHiveMetastoreConfig setGlueStsEndpointUrl(String glueStsEndpointUrl)
    {
        this.glueStsEndpointUrl = Optional.ofNullable(glueStsEndpointUrl);
        return this;
    }

    @Min(1)
    @Max(1000)
    public int getMaxGlueConnections()
    {
        return maxGlueConnections;
    }

    @Config("hive.metastore.glue.max-connections")
    @ConfigDescription("Max number of concurrent connections to Glue")
    public GlueHiveMetastoreConfig setMaxGlueConnections(int maxGlueConnections)
    {
        this.maxGlueConnections = maxGlueConnections;
        return this;
    }

    @Min(0)
    public int getMaxGlueErrorRetries()
    {
        return maxGlueErrorRetries;
    }

    @Config("hive.metastore.glue.max-error-retries")
    public GlueHiveMetastoreConfig setMaxGlueErrorRetries(int maxGlueErrorRetries)
    {
        this.maxGlueErrorRetries = maxGlueErrorRetries;
        return this;
    }

    public Optional<String> getDefaultWarehouseDir()
    {
        return defaultWarehouseDir;
    }

    @Config("hive.metastore.glue.default-warehouse-dir")
    @ConfigDescription("Hive Glue metastore default warehouse directory")
    public GlueHiveMetastoreConfig setDefaultWarehouseDir(String defaultWarehouseDir)
    {
        this.defaultWarehouseDir = Optional.ofNullable(defaultWarehouseDir);
        return this;
    }

    public Optional<String> getCatalogId()
    {
        return catalogId;
    }

    @Config("hive.metastore.glue.catalogid")
    @ConfigDescription("Hive Glue metastore catalog id")
    public GlueHiveMetastoreConfig setCatalogId(String catalogId)
    {
        this.catalogId = Optional.ofNullable(catalogId);
        return this;
    }

    @Min(1)
    @Max(10)
    public int getPartitionSegments()
    {
        return partitionSegments;
    }

    @Config("hive.metastore.glue.partitions-segments")
    @ConfigDescription("Number of segments for partitioned Glue tables")
    public GlueHiveMetastoreConfig setPartitionSegments(int partitionSegments)
    {
        this.partitionSegments = partitionSegments;
        return this;
    }

    @Min(1)
    @Max(1000)
    public int getGetPartitionThreads()
    {
        return getPartitionThreads;
    }

    @Config("hive.metastore.glue.get-partition-threads")
    @ConfigDescription("Number of threads for parallel partition fetches from Glue")
    public GlueHiveMetastoreConfig setGetPartitionThreads(int getPartitionThreads)
    {
        this.getPartitionThreads = getPartitionThreads;
        return this;
    }

    public Optional<String> getIamRole()
    {
        return iamRole;
    }

    @Config("hive.metastore.glue.iam-role")
    @ConfigDescription("IAM role to assume when connecting to the Hive Glue metastore")
    public GlueHiveMetastoreConfig setIamRole(String iamRole)
    {
        this.iamRole = Optional.ofNullable(iamRole);
        return this;
    }

    public Optional<String> getAwsAccessKey()
    {
        return awsAccessKey;
    }

    @Config("hive.metastore.glue.aws-access-key")
    @ConfigDescription("Hive Glue metastore AWS access key")
    public GlueHiveMetastoreConfig setAwsAccessKey(String awsAccessKey)
    {
        this.awsAccessKey = Optional.ofNullable(awsAccessKey);
        return this;
    }

    public Optional<String> getAwsSecretKey()
    {
        return awsSecretKey;
    }

    @Config("hive.metastore.glue.aws-secret-key")
    @ConfigDescription("Hive Glue metastore AWS secret key")
    @ConfigSecuritySensitive
    public GlueHiveMetastoreConfig setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = Optional.ofNullable(awsSecretKey);
        return this;
    }

    public boolean isColumnStatisticsEnabled()
    {
        return columnStatisticsEnabled;
    }

    @Config("hive.metastore.glue.column-statistics-enabled")
    @ConfigDescription("Enable use of column statistics on Glue Metastore")
    public GlueHiveMetastoreConfig setColumnStatisticsEnabled(boolean columnStatisticsEnabled)
    {
        this.columnStatisticsEnabled = columnStatisticsEnabled;
        return this;
    }

    @Min(1)
    public int getReadStatisticsThreads()
    {
        return readStatisticsThreads;
    }

    @Config("hive.metastore.glue.read-statistics-threads")
    @ConfigDescription("Number of threads for parallel statistics reads from Glue")
    public GlueHiveMetastoreConfig setReadStatisticsThreads(int getReadStatisticsThreads)
    {
        this.readStatisticsThreads = getReadStatisticsThreads;
        return this;
    }

    @Min(1)
    public int getWriteStatisticsThreads()
    {
        return writeStatisticsThreads;
    }

    @Config("hive.metastore.glue.write-statistics-threads")
    @ConfigDescription("Number of threads for parallel statistics writes to Glue")
    public GlueHiveMetastoreConfig setWriteStatisticsThreads(int writeStatisticsThreads)
    {
        this.writeStatisticsThreads = writeStatisticsThreads;
        return this;
    }

    @Min(0)
    public int getMaxUnprocessedKeysRetries()
    {
        return maxUnprocessedKeysRetries;
    }

    @Config("hive.metastore.glue.max-unprocessed-keys-retries")
    @ConfigDescription("Maximum number of retry attempts for unprocessed partition keys in batch get operations")
    public GlueHiveMetastoreConfig setMaxUnprocessedKeysRetries(int maxUnprocessedKeysRetries)
    {
        this.maxUnprocessedKeysRetries = maxUnprocessedKeysRetries;
        return this;
    }

    public Duration getUnprocessedKeysRetryMinDelay()
    {
        return unprocessedKeysRetryMinDelay;
    }

    @Config("hive.metastore.glue.unprocessed-keys-retry-min-delay")
    @ConfigDescription("Minimum delay between retries for unprocessed partition keys (exponential backoff starts from this value)")
    public GlueHiveMetastoreConfig setUnprocessedKeysRetryMinDelay(Duration unprocessedKeysRetryMinDelay)
    {
        this.unprocessedKeysRetryMinDelay = unprocessedKeysRetryMinDelay;
        return this;
    }

    public Duration getUnprocessedKeysRetryMaxDelay()
    {
        return unprocessedKeysRetryMaxDelay;
    }

    @Config("hive.metastore.glue.unprocessed-keys-retry-max-delay")
    @ConfigDescription("Maximum delay between retries for unprocessed partition keys (exponential backoff is capped at this value)")
    public GlueHiveMetastoreConfig setUnprocessedKeysRetryMaxDelay(Duration unprocessedKeysRetryMaxDelay)
    {
        this.unprocessedKeysRetryMaxDelay = unprocessedKeysRetryMaxDelay;
        return this;
    }

    public boolean isFailOnMissingPartitionInStatisticsUpdate()
    {
        return failOnMissingPartitionInStatisticsUpdate;
    }

    @Config("hive.metastore.glue.fail-on-missing-partition-in-statistics-update")
    @ConfigDescription("Fail partition statistics update if any requested partition is missing or dropped")
    public GlueHiveMetastoreConfig setFailOnMissingPartitionInStatisticsUpdate(boolean failOnMissingPartitionInStatisticsUpdate)
    {
        this.failOnMissingPartitionInStatisticsUpdate = failOnMissingPartitionInStatisticsUpdate;
        return this;
    }
}
