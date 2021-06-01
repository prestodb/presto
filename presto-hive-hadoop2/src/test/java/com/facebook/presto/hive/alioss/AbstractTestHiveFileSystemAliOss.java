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
package com.facebook.presto.hive.alioss;

import com.facebook.presto.hive.AbstractTestHiveFileSystem;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.fs.Path;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.testng.util.Strings.isNullOrEmpty;

public abstract class AbstractTestHiveFileSystemAliOss
        extends AbstractTestHiveFileSystem
{
    private String aliossAccessKeyId;
    private String aliossAccessKeySecret;
    private String aliossEndpoint;
    private String writableBucket;

    protected void setup(String host, int port, String databaseName, String aliossAccessKeyId, String aliossAccessKeySecret, String aliossEndpoint, String writableBucket)
    {
        checkArgument(!isNullOrEmpty(host), "Expected non empty host");
        checkArgument(!isNullOrEmpty(databaseName), "Expected non empty databaseName");
        checkArgument(!isNullOrEmpty(aliossAccessKeyId), "Expected non empty aliossAccessKeyId");
        checkArgument(!isNullOrEmpty(aliossAccessKeySecret), "Expected non empty aliossAccessKeySecret");
        checkArgument(!isNullOrEmpty(aliossEndpoint), "Expected non empty aliossEndpoint");
        checkArgument(!isNullOrEmpty(writableBucket), "Expected non empty writableBucket");
        this.aliossAccessKeyId = aliossAccessKeyId;
        this.aliossAccessKeySecret = aliossAccessKeySecret;
        this.aliossEndpoint = aliossEndpoint;
        this.writableBucket = writableBucket;

        super.setup(host, port, databaseName, this::createHdfsConfiguration, false);
    }

    HdfsConfiguration createHdfsConfiguration(HiveClientConfig config, MetastoreClientConfig metastoreConfig)
    {
        AliOssConfigurationInitializer aliOssConfigurationInitializer = new HiveAliOssConfigurationInitializer(
                new HiveAliOssConfig()
                        .setAccessKeyId(aliossAccessKeyId)
                        .setAccessKeySecret(aliossAccessKeySecret)
                        .setEndpoint(aliossEndpoint));
        return new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config, metastoreConfig, ignored -> {}, ignored -> {}, aliOssConfigurationInitializer), ImmutableSet.of());
    }

    @Override
    protected Path getBasePath()
    {
        return new Path(format("oss://%s/", writableBucket));
    }
}
