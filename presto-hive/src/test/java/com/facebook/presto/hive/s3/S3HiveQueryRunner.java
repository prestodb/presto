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
package com.facebook.presto.hive.s3;

import com.facebook.presto.hive.HiveCommonClientConfig;
import com.facebook.presto.hive.HiveQueryRunner;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.TestingHiveCluster;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastoreConfig;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;

import java.io.File;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;

public final class S3HiveQueryRunner
{
    private S3HiveQueryRunner() {}

    public static DistributedQueryRunner create(
            HostAndPort hiveEndpoint,
            HostAndPort s3Endpoint,
            String s3AccessKey,
            String s3SecretKey,
            Map<String, String> additionalHiveProperties,
            Map<String, String> additionalHiveClientProperties)
            throws Exception
    {
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        ThriftHiveMetastoreConfig thriftHiveMetastoreConfig = new ThriftHiveMetastoreConfig();
        if (!additionalHiveClientProperties.isEmpty()) {
            thriftHiveMetastoreConfig.setTlsEnabled(Boolean.parseBoolean(additionalHiveClientProperties.get("hive.metastore.thrift.client.tls.enabled")));
            if (additionalHiveClientProperties.get("hive.metastore.thrift.client.tls.keystore-path") != null) {
                thriftHiveMetastoreConfig.setKeystorePath(new File(additionalHiveClientProperties.get("hive.metastore.thrift.client.tls.keystore-path")));
                thriftHiveMetastoreConfig.setKeystorePassword(additionalHiveClientProperties.get("hive.metastore.thrift.client.tls.keystore-password"));
            }
            if (additionalHiveClientProperties.get("hive.metastore.thrift.client.tls.truststore-path") != null) {
                thriftHiveMetastoreConfig.setTruststorePath(new File(additionalHiveClientProperties.get("hive.metastore.thrift.client.tls.truststore-path")));
                thriftHiveMetastoreConfig.setTrustStorePassword(additionalHiveClientProperties.get("hive.metastore.thrift.client.tls.truststore-password"));
            }
        }
        return HiveQueryRunner.createQueryRunner(ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(),
                ImmutableMap.of(), "sql-standard",
                ImmutableMap.<String, String>builder()
                        .put("hive.s3.endpoint", "http://" + s3Endpoint)
                        .put("hive.s3.aws-access-key", s3AccessKey)
                        .put("hive.s3.aws-secret-key", s3SecretKey)
                        .putAll(additionalHiveProperties)
                        .build(),
                Optional.of(1),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new BridgingHiveMetastore(
                        new ThriftHiveMetastore(
                                new TestingHiveCluster(metastoreClientConfig,
                                        thriftHiveMetastoreConfig,
                                        hiveEndpoint.getHost(),
                                        hiveEndpoint.getPort(), new HiveCommonClientConfig()), metastoreClientConfig,
                                HDFS_ENVIRONMENT),
                        new HivePartitionMutator())),
                ImmutableMap.of());
    }
}
