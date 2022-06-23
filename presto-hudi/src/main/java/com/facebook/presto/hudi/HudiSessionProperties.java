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

package com.facebook.presto.hudi;

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.hive.HiveSessionProperties.CACHE_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.dataSizeSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;

public class HudiSessionProperties
{
    private final List<PropertyMetadata<?>> sessionProperties;

    private static final String HUDI_METADATA_TABLE_ENABLED = "hudi_metadata_table_enabled";

    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_BATCH_READ_OPTIMIZATION_ENABLED = "parquet_batch_read_optimization_enabled";
    private static final String PARQUET_BATCH_READER_VERIFICATION_ENABLED = "parquet_batch_reader_verification_enabled";

    @Inject
    public HudiSessionProperties(HiveClientConfig hiveClientConfig, HudiConfig hudiConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        HUDI_METADATA_TABLE_ENABLED,
                        "Enable Hudi MetaTable Table",
                        hudiConfig.isMetadataTableEnabled(),
                        false),
                booleanProperty(
                        // required by presto-hive module, might be removed in future
                        CACHE_ENABLED,
                        "Enable cache for hive",
                        false,
                        true),
                dataSizeSessionProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        hiveClientConfig.getParquetMaxReadBlockSize(),
                        false),
                booleanProperty(
                        PARQUET_BATCH_READ_OPTIMIZATION_ENABLED,
                        "Is Parquet batch read optimization enabled",
                        false,
                        false),
                booleanProperty(
                        PARQUET_BATCH_READER_VERIFICATION_ENABLED,
                        "Is Parquet batch reader verification enabled? This is for testing purposes only, not to be used in production",
                        false,
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isHudiMetadataTableEnabled(ConnectorSession session)
    {
        return session.getProperty(HUDI_METADATA_TABLE_ENABLED, Boolean.class);
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
}
