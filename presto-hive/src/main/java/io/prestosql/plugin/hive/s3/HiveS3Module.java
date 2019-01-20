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
package io.prestosql.plugin.hive.s3;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.hive.HiveClientConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HiveS3Module
        extends AbstractConfigurationAwareModule
{
    private static final String EMR_FS_CLASS_NAME = "com.amazon.ws.emr.hadoop.fs.EmrFileSystem";

    @Override
    protected void setup(Binder binder)
    {
        S3FileSystemType type = buildConfigObject(HiveClientConfig.class).getS3FileSystemType();
        if (type == S3FileSystemType.PRESTO) {
            binder.bind(S3ConfigurationUpdater.class).to(PrestoS3ConfigurationUpdater.class).in(Scopes.SINGLETON);
            configBinder(binder).bindConfig(HiveS3Config.class);

            binder.bind(PrestoS3FileSystemStats.class).toInstance(PrestoS3FileSystem.getFileSystemStats());
            newExporter(binder).export(PrestoS3FileSystemStats.class)
                    .as(generator -> generator.generatedNameOf(PrestoS3FileSystem.class));
        }
        else if (type == S3FileSystemType.EMRFS) {
            validateEmrFsClass();
            binder.bind(S3ConfigurationUpdater.class).to(EmrFsS3ConfigurationUpdater.class).in(Scopes.SINGLETON);
        }
        else {
            throw new RuntimeException("Unknown file system type: " + type);
        }
    }

    private static void validateEmrFsClass()
    {
        // verify that the class exists
        try {
            Class.forName(EMR_FS_CLASS_NAME, true, JavaUtils.getClassLoader());
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("EMR File System class not found: " + EMR_FS_CLASS_NAME, e);
        }
    }

    public static class EmrFsS3ConfigurationUpdater
            implements S3ConfigurationUpdater
    {
        @Override
        public void updateConfiguration(Configuration config)
        {
            // re-map filesystem schemes to use the Amazon EMR file system
            config.set("fs.s3.impl", EMR_FS_CLASS_NAME);
            config.set("fs.s3a.impl", EMR_FS_CLASS_NAME);
            config.set("fs.s3n.impl", EMR_FS_CLASS_NAME);
        }
    }
}
