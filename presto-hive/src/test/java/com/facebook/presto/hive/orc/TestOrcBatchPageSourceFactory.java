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
package com.facebook.presto.hive.orc;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hive.DwrfEncryptionMetadata;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveDwrfEncryptionProvider;
import com.facebook.presto.hive.HiveFileSplit;
import com.facebook.presto.hive.TestOrcBatchPageSourceMemoryTracking;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveFileContext.DEFAULT_HIVE_FILE_CONTEXT;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestOrcBatchPageSourceFactory
{
    @Test
    public void testCreateOrcPageSourceException() throws Exception
    {
        List<TestOrcBatchPageSourceMemoryTracking.TestColumn> testColumns = ImmutableList.<TestOrcBatchPageSourceMemoryTracking.TestColumn>builder()
                .add(new TestOrcBatchPageSourceMemoryTracking.TestColumn("p_empty_string", javaStringObjectInspector, () -> "", true))
                .add(new TestOrcBatchPageSourceMemoryTracking.TestColumn("p_string", javaStringObjectInspector, () -> Long.toHexString(893247L), false))
                .build();
        Path tempFilePath = Paths.get(System.getProperty("java.io.tmpdir"),
                "presto_test_orc_batch_page_source_factory", "temp.orc");
        ImmutableList.Builder<HiveColumnHandle> columnsBuilder = ImmutableList.builder();
        Configuration configuration = new JobConf(new Configuration(false));
        try {
            FileSplit fileSplit = TestOrcBatchPageSourceMemoryTracking.createTestFile(tempFilePath.toAbsolutePath().toString(),
                    new OrcOutputFormat(), new OrcSerde(), null, testColumns, 1, 1);
            HiveFileSplit hiveFileSplit = new HiveFileSplit(
                    fileSplit.getPath().toString(),
                    fileSplit.getStart(),
                    fileSplit.getLength(),
                    fileSplit.getLength(),
                    Instant.now().toEpochMilli(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    0);
            OrcReaderOptions orcReaderOptions = null; //need to be set to null to trigger PrestoException
            Optional<EncryptionInformation> encryptionInformation =
                    Optional.of(EncryptionInformation.fromEncryptionMetadata(DwrfEncryptionMetadata.forPerField(
                            ImmutableMap.of("field1", "test1".getBytes()),
                            ImmutableMap.of(),
                            "test_algo",
                            "test_provider")));
            try (ConnectorPageSource connectorPageSource = OrcBatchPageSourceFactory.createOrcPageSource(ORC, HDFS_ENVIRONMENT,
                    configuration, hiveFileSplit, columnsBuilder.build(), true,
                    TupleDomain.all(), DateTimeZone.getDefault(), FUNCTION_AND_TYPE_MANAGER, true,
                    new FileFormatDataSourceStats(), 100, new StorageOrcFileTailSource(),
                    StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()), DEFAULT_HIVE_FILE_CONTEXT, orcReaderOptions,
                    encryptionInformation, HiveDwrfEncryptionProvider.NO_ENCRYPTION.toDwrfEncryptionProvider(), SESSION, Optional.empty())) {
                fail("createOrcPageSource call should have resulted in an PrestoException.");
            }
            catch (PrestoException pe) {
                assertTrue(pe.getMessage().contains("orcReaderOptions is null"), "Incorrect PrestoException was thrown.");
            }
        }
        finally {
            Files.deleteIfExists(tempFilePath);
        }
    }
}
