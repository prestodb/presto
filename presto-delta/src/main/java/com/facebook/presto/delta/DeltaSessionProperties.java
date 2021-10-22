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
package com.facebook.presto.delta;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.hive.HiveSessionProperties.dataSizeSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;

public final class DeltaSessionProperties
{
    private static final String CACHE_ENABLED = "cache_enabled";
    private static final String PARQUET_FAIL_WITH_CORRUPTED_STATISTICS = "parquet_fail_with_corrupted_statistics";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_BATCH_READ_OPTIMIZATION_ENABLED = "parquet_batch_read_optimization_enabled";
    private static final String PARQUET_BATCH_READER_VERIFICATION_ENABLED = "parquet_batch_reader_verification_enabled";
    public static final String PARQUET_DEREFERENCE_PUSHDOWN_ENABLED = "parquet_dereference_pushdown_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DeltaSessionProperties(
            DeltaConfig deltaConfigConfig,
            HiveClientConfig hiveClientConfig,
            CacheConfig cacheConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        CACHE_ENABLED,
                        "Enable cache for Delta tables",
                        cacheConfig.isCachingEnabled(),
                        false),
                booleanProperty(
                        PARQUET_FAIL_WITH_CORRUPTED_STATISTICS,
                        "Parquet: Fail when scanning Parquet files with corrupted statistics",
                        hiveClientConfig.isFailOnCorruptedParquetStatistics(),
                        false),
                dataSizeSessionProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        hiveClientConfig.getParquetMaxReadBlockSize(),
                        false),
                booleanProperty(
                        PARQUET_BATCH_READ_OPTIMIZATION_ENABLED,
                        "Is Parquet batch read optimization enabled",
                        hiveClientConfig.isParquetBatchReadOptimizationEnabled(),
                        false),
                booleanProperty(
                        PARQUET_BATCH_READER_VERIFICATION_ENABLED,
                        "Is Parquet batch reader verification enabled? This is for testing purposes only, not to be used in production",
                        hiveClientConfig.isParquetBatchReaderVerificationEnabled(),
                        false),
                booleanProperty(
                        PARQUET_DEREFERENCE_PUSHDOWN_ENABLED,
                        "Is dereference pushdown expression pushdown into Parquet reader enabled?",
                        deltaConfigConfig.isParquetDereferencePushdownEnabled(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(CACHE_ENABLED, Boolean.class);
    }

    public static boolean isFailOnCorruptedParquetStatistics(ConnectorSession session)
    {
        return session.getProperty(PARQUET_FAIL_WITH_CORRUPTED_STATISTICS, Boolean.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static boolean isParquetBatchReadsEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, Boolean.class);
    }

    public static boolean isParquetBatchReaderVerificationEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_BATCH_READER_VERIFICATION_ENABLED, Boolean.class);
    }

    public static boolean isParquetDereferencePushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, Boolean.class);
    }
}
