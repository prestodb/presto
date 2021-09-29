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
package com.facebook.presto.iceberg;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.connector.ConnectorMetadata;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class IcebergMetadataFactory
{
    private final ExtendedHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final IcebergResourceFactory resourceFactory;
    private final boolean nativeCatalogMode;

    @Inject
    public IcebergMetadataFactory(
            IcebergConfig config,
            IcebergResourceFactory resourceFactory,
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec)
    {
        this.resourceFactory = requireNonNull(resourceFactory, "resourceFactory is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.nativeCatalogMode = requireNonNull(config, "config is null").isNativeMode();
    }

    public ConnectorMetadata create()
    {
        if (nativeCatalogMode) {
            return new IcebergNativeMetadata(resourceFactory, typeManager, commitTaskCodec);
        }
        return new IcebergMetadata(metastore, hdfsEnvironment, typeManager, commitTaskCodec);
    }
}
