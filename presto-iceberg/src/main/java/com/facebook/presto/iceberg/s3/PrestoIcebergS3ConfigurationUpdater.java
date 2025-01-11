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
package com.facebook.presto.iceberg.s3;

import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergConfig;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;

public class PrestoIcebergS3ConfigurationUpdater
        extends PrestoS3ConfigurationUpdater
{
    private final IcebergConfig icebergConfig;

    @Inject
    public PrestoIcebergS3ConfigurationUpdater(HiveS3Config config, IcebergConfig icebergConfig)
    {
        super(config);
        this.icebergConfig = icebergConfig;
    }

    @Override
    public void updateConfiguration(Configuration config)
    {
        super.updateConfiguration(config);

        if (this.icebergConfig.getCatalogType().equals(HADOOP)) {
            // re-map filesystem schemes to match Amazon Elastic MapReduce
            config.set("fs.s3.impl", PrestoIcebergNativeS3FileSystem.class.getName());
            config.set("fs.s3a.impl", PrestoIcebergNativeS3FileSystem.class.getName());
            config.set("fs.s3n.impl", PrestoIcebergNativeS3FileSystem.class.getName());
        }
    }
}
