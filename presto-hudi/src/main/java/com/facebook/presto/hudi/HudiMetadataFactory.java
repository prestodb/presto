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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.connector.ConnectorMetadata;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class HudiMetadataFactory
{
    private final ExtendedHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final long perTransactionCacheMaximumSize;
    private final boolean metastoreImpersonationEnabled;
    private final int metastorePartitionCacheMaxColumnCount;

    @Inject
    public HudiMetadataFactory(
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            MetastoreClientConfig metastoreClientConfig)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.perTransactionCacheMaximumSize = metastoreClientConfig.getPerTransactionMetastoreCacheMaximumSize();
        this.metastoreImpersonationEnabled = metastoreClientConfig.isMetastoreImpersonationEnabled();
        this.metastorePartitionCacheMaxColumnCount = metastoreClientConfig.getPartitionCacheColumnCountLimit();
    }

    public ConnectorMetadata create()
    {
        return new HudiMetadata(
                CachingHiveMetastore.memoizeMetastore(metastore, metastoreImpersonationEnabled, perTransactionCacheMaximumSize, metastorePartitionCacheMaxColumnCount),
                hdfsEnvironment,
                typeManager);
    }
}
