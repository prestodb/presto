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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.hive.orc.OrcSelectivePageSource;
import com.facebook.presto.hive.orc.OrcSelectivePageSourceFactory;
import com.facebook.presto.hive.orc.TupleDomainFilterCache;
import com.facebook.presto.hive.util.ConfigurationUtils;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.spi.relation.RowExpression;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTimeZone;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TestOrcSelectivePageSourceFactory
{
    @Test
    public void testSupplyRowIDs()
    {
 /*       HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        HdfsEnvironment testHdfsEnvironment = HiveTestUtils.HDFS_ENVIRONMENT;
        OrcSelectivePageSourceFactory factory = HiveTestUtils.makeOrcSelectivePageSourceFactory(hiveClientConfig, testHdfsEnvironment, stats);
*/

        List<Integer> outputColumns = ImmutableList.of(1);
        ArrayList<HiveColumnHandle> selectedColumns = new ArrayList<>();
        selectedColumns.add(HiveColumnHandle.rowIdColumnHandle());
        HiveFileSplit fileSplit = new HiveFileSplit("path", 0, 100, 1000, 2000, Optional.empty(), Collections.emptyMap(), 54L);
        Configuration configuration = ConfigurationUtils.getInitialConfiguration();
        RowExpression rowExpression = LogicalRowExpressions.TRUE_CONSTANT;
        OrcSelectivePageSource pageSource = (OrcSelectivePageSource) OrcSelectivePageSourceFactory.createOrcPageSource(
                HiveTestUtils.SESSION,
                OrcEncoding.ORC,
                HiveTestUtils.HDFS_ENVIRONMENT,
                configuration,
                fileSplit,
                selectedColumns, // List<HiveColumnHandle> selectedColumns,
                Collections.emptyMap(),
                Collections.emptyMap(), // need to add row ID coercers
                Optional.empty(),
                outputColumns,
                TupleDomain.all(),
                rowExpression, // remainingPredicate
                false, // boolean useOrcColumnNames,
                DateTimeZone.UTC, // DateTimeZone hiveStorageTimeZone,
                HiveTestUtils.FUNCTION_AND_TYPE_MANAGER,
                HiveTestUtils.FUNCTION_RESOLUTION,
                HiveTestUtils.ROW_EXPRESSION_SERVICE,
                false, // boolean orcBloomFiltersEnabled,
                new FileFormatDataSourceStats(), // FileFormatDataSourceStats stats,
                10, // int domainCompactionThreshold,
                new StorageOrcFileTailSource(),
                StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()),
                HiveFileContext.DEFAULT_HIVE_FILE_CONTEXT,
                new TupleDomainFilterCache(),
                Optional.empty(), // Optional<EncryptionInformation> encryptionInformation
                DwrfEncryptionProvider.NO_ENCRYPTION,
                false, // boolean appendRowNumberEnabled
                Optional.of(new byte[17])); // rowIDPartitionComponent)
    }
}
