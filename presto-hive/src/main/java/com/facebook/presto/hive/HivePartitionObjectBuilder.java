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

import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.hive.HiveSessionProperties.getNewPartitionUserSuppliedParameter;

public class HivePartitionObjectBuilder
        implements PartitionObjectBuilder
{
    @Override
    public Partition buildPartitionObject(
            ConnectorSession session,
            Table table,
            PartitionUpdate partitionUpdate,
            String prestoVersion,
            Map<String, String> extraParameters)
    {
        ImmutableMap.Builder extraParametersBuilder = ImmutableMap.builder()
                .put(HiveMetadata.PRESTO_VERSION_NAME, prestoVersion)
                .put(MetastoreUtil.PRESTO_QUERY_ID_NAME, session.getQueryId())
                .putAll(extraParameters);
        getNewPartitionUserSuppliedParameter(session)
                .ifPresent(
                        param -> extraParametersBuilder.put("user_supplied", param));

        return Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(MetastoreUtil.extractPartitionValues(partitionUpdate.getName()))
                .setParameters(extraParametersBuilder.build())
                .withStorage(storage -> storage
                        .setStorageFormat(HiveSessionProperties.isRespectTableFormat(session) ?
                                table.getStorage().getStorageFormat() :
                                StorageFormat.fromHiveStorageFormat(HiveSessionProperties.getHiveStorageFormat(session)))
                        .setLocation(partitionUpdate.getTargetPath().toString())
                        .setBucketProperty(table.getStorage().getBucketProperty())
                        .setSerdeParameters(table.getStorage().getSerdeParameters())
                        .setParameters(table.getStorage().getParameters()))
                .build();
    }
}
