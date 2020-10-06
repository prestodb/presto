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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveTestUtils.METADATA;
import static org.testng.Assert.assertFalse;

public class TestParquetPageSourceFactory
{
    private static final String PARQUET_HIVE_SERDE = "parquet.hive.serde.ParquetHiveSerDe";

    private ParquetPageSourceFactory parquetPageSourceFactory;
    private final StandardFunctionResolution functionResolution = new FunctionResolution(METADATA.getFunctionAndTypeManager());

    @BeforeClass
    public void setUp()
    {
        HiveHdfsConfiguration hiveHdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HiveClientConfig(), new MetastoreClientConfig()), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hiveHdfsConfiguration, new MetastoreClientConfig(), new NoHdfsAuthentication());
        parquetPageSourceFactory = new ParquetPageSourceFactory(new TestingTypeManager(), functionResolution, hdfsEnvironment, new FileFormatDataSourceStats(), new MetadataReader());
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp()
    {
        parquetPageSourceFactory = null;
    }

    @Test
    public void testCreatePageSourceEmptyWithoutParquetSerDe()
    {
        StorageFormat storageFormat = StorageFormat.create("random.test.serde", "random.test.inputformat", "");
        Storage storage = new Storage(storageFormat, "test", Optional.empty(), true, ImmutableMap.of(), ImmutableMap.of());
        Optional<? extends ConnectorPageSource> optionalPageSource = parquetPageSourceFactory.createPageSource(new Configuration(), null, null, 0L, 0L, 0L, storage, null, null, null, null, null, null, Optional.empty());
        assertFalse(optionalPageSource.isPresent());
    }

    @Test
    public void testCreatePageSourceEmptyWithParquetSerDeAndAnnotation()
    {
        StorageFormat storageFormat = StorageFormat.create(PARQUET_HIVE_SERDE, HoodieParquetRealtimeInputFormat.class.getName(), "");
        Storage storage = new Storage(storageFormat, "test", Optional.empty(), true, ImmutableMap.of(), ImmutableMap.of());
        Optional<? extends ConnectorPageSource> optionalPageSource = parquetPageSourceFactory.createPageSource(new Configuration(), null, null, 0L, 0L, 0L, storage, null, null, null, null, null, null, Optional.empty());
        assertFalse(optionalPageSource.isPresent());
    }
}
