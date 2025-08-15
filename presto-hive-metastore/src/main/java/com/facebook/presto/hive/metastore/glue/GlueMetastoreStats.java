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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.hive.aws.AbstractSdkMetricsCollector;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class GlueMetastoreStats
{
    private final GlueCatalogApiStats createDatabase = new GlueCatalogApiStats();
    private final GlueCatalogApiStats updateDatabase = new GlueCatalogApiStats();
    private final GlueCatalogApiStats deleteDatabase = new GlueCatalogApiStats();
    private final GlueCatalogApiStats getDatabases = new GlueCatalogApiStats();
    private final GlueCatalogApiStats getDatabase = new GlueCatalogApiStats();
    private final GlueCatalogApiStats createTable = new GlueCatalogApiStats();
    private final GlueCatalogApiStats updateTable = new GlueCatalogApiStats();
    private final GlueCatalogApiStats deleteTable = new GlueCatalogApiStats();
    private final GlueCatalogApiStats getTables = new GlueCatalogApiStats();
    private final GlueCatalogApiStats getTable = new GlueCatalogApiStats();
    private final GlueCatalogApiStats batchCreatePartitions = new GlueCatalogApiStats();
    private final GlueCatalogApiStats batchGetPartitions = new GlueCatalogApiStats();
    private final GlueCatalogApiStats updatePartition = new GlueCatalogApiStats();
    private final GlueCatalogApiStats deletePartition = new GlueCatalogApiStats();
    private final GlueCatalogApiStats getPartitions = new GlueCatalogApiStats();
    private final GlueCatalogApiStats getPartition = new GlueCatalogApiStats();

    // see AWSRequestMetrics
    private final CounterStat awsRequestCount = new CounterStat();
    private final CounterStat awsRetryCount = new CounterStat();
    private final CounterStat awsThrottleExceptions = new CounterStat();
    private final TimeStat awsRequestTime = new TimeStat(MILLISECONDS);
    private final TimeStat awsClientExecuteTime = new TimeStat(MILLISECONDS);
    private final TimeStat awsClientRetryPauseTime = new TimeStat(MILLISECONDS);

    @Managed
    @Nested
    public GlueCatalogApiStats getCreateDatabase()
    {
        return createDatabase;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getUpdateDatabase()
    {
        return updateDatabase;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getDeleteDatabase()
    {
        return deleteDatabase;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getGetDatabases()
    {
        return getDatabases;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getGetDatabase()
    {
        return getDatabase;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getCreateTable()
    {
        return createTable;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getUpdateTable()
    {
        return updateTable;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getDeleteTable()
    {
        return deleteTable;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getGetTables()
    {
        return getTables;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getGetTable()
    {
        return getTable;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getBatchCreatePartitions()
    {
        return batchCreatePartitions;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getBatchGetPartitions()
    {
        return batchGetPartitions;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getUpdatePartition()
    {
        return updatePartition;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getDeletePartition()
    {
        return deletePartition;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getGetPartitions()
    {
        return getPartitions;
    }

    @Managed
    @Nested
    public GlueCatalogApiStats getGetPartition()
    {
        return getPartition;
    }

    @Managed
    @Nested
    public CounterStat getAwsRequestCount()
    {
        return awsRequestCount;
    }

    @Managed
    @Nested
    public CounterStat getAwsRetryCount()
    {
        return awsRetryCount;
    }

    @Managed
    @Nested
    public CounterStat getAwsThrottleExceptions()
    {
        return awsThrottleExceptions;
    }

    @Managed
    @Nested
    public TimeStat getAwsRequestTime()
    {
        return awsRequestTime;
    }

    @Managed
    @Nested
    public TimeStat getAwsClientExecuteTime()
    {
        return awsClientExecuteTime;
    }

    @Managed
    @Nested
    public TimeStat getAwsClientRetryPauseTime()
    {
        return awsClientRetryPauseTime;
    }

    public GlueSdkClientMetricsCollector newRequestMetricsCollector()
    {
        return new GlueSdkClientMetricsCollector(this);
    }

    public static class GlueSdkClientMetricsCollector
            extends AbstractSdkMetricsCollector
    {
        private final GlueMetastoreStats stats;

        public GlueSdkClientMetricsCollector(GlueMetastoreStats stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        protected void recordRequestCount(long count)
        {
            stats.awsRequestCount.update(count);
        }

        @Override
        protected void recordRetryCount(long count)
        {
            stats.awsRetryCount.update(count);
        }

        @Override
        protected void recordThrottleExceptionCount(long count)
        {
            stats.awsThrottleExceptions.update(count);
        }

        @Override
        protected void recordHttpRequestTime(Duration duration)
        {
            stats.awsRequestTime.add(duration);
        }

        @Override
        protected void recordClientExecutionTime(Duration duration)
        {
            stats.awsClientExecuteTime.add(duration);
        }

        @Override
        protected void recordRetryPauseTime(Duration duration)
        {
            stats.awsClientRetryPauseTime.add(duration);
        }
    }
}
