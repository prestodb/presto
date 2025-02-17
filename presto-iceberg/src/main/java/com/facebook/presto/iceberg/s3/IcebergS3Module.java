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

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.HiveS3Module;
import com.facebook.presto.hive.s3.PrestoS3FileSystem;
import com.facebook.presto.hive.s3.PrestoS3FileSystemStats;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import com.facebook.presto.hive.s3.S3FileSystemType;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergS3Module
        extends HiveS3Module
{
    public IcebergS3Module(String connectorId)
    {
        super(connectorId);
    }

    @Override
    protected void setup(Binder binder)
    {
        S3FileSystemType type = buildConfigObject(HiveClientConfig.class).getS3FileSystemType();
        if (type == S3FileSystemType.PRESTO) {
            bindSecurityMapping(binder);

            binder.bind(S3ConfigurationUpdater.class).to(PrestoIcebergS3ConfigurationUpdater.class).in(Scopes.SINGLETON);
            configBinder(binder).bindConfig(HiveS3Config.class);

            binder.bind(PrestoS3FileSystemStats.class).toInstance(PrestoS3FileSystem.getFileSystemStats());
            newExporter(binder).export(PrestoS3FileSystemStats.class).as(generatedNameOf(PrestoS3FileSystem.class, connectorId));
        }
        else if (type == S3FileSystemType.EMRFS) {
            validateEmrFsClass();
            binder.bind(S3ConfigurationUpdater.class).to(EmrFsS3ConfigurationUpdater.class).in(Scopes.SINGLETON);
        }
        else if (type == S3FileSystemType.HADOOP_DEFAULT) {
            // configuration is done using Hadoop configuration files
            binder.bind(S3ConfigurationUpdater.class).to(HadoopDefaultConfigurationUpdater.class).in(Scopes.SINGLETON);
        }
        else {
            throw new RuntimeException("Unknown file system type: " + type);
        }
    }
}
