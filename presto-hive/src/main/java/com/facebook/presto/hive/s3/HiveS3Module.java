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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.aws.security.AWSSecurityMappingConfig;
import com.facebook.presto.hive.aws.security.AWSSecurityMappingsSupplier;
import com.facebook.presto.hive.s3.security.AWSS3SecurityMappingConfigurationProvider;
import com.facebook.presto.hive.s3.security.ForAWSS3DynamicConfigurationProvider;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.presto.hive.aws.security.AWSSecurityMappingType.S3;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HiveS3Module
        extends AbstractConfigurationAwareModule
{
    private static final String EMR_FS_CLASS_NAME = "com.amazon.ws.emr.hadoop.fs.EmrFileSystem";

    private final String connectorId;

    public HiveS3Module(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        S3FileSystemType type = buildConfigObject(HiveClientConfig.class).getS3FileSystemType();
        if (type == S3FileSystemType.PRESTO) {
            bindSecurityMapping(binder);

            binder.bind(S3ConfigurationUpdater.class).to(PrestoS3ConfigurationUpdater.class).in(Scopes.SINGLETON);
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

    @Provides
    @Singleton
    @ForAWSS3DynamicConfigurationProvider
    public AWSSecurityMappingsSupplier provideAWSSecurityMappingsSupplier(AWSSecurityMappingConfig config)
    {
        return new AWSSecurityMappingsSupplier(config.getConfigFile(), config.getRefreshPeriod());
    }

    private void bindSecurityMapping(Binder binder)
    {
        if (buildConfigObject(AWSSecurityMappingConfig.class).getConfigFile().isPresent() &&
                buildConfigObject(AWSSecurityMappingConfig.class).getMappingType().equals(S3)) {
            newSetBinder(binder, DynamicConfigurationProvider.class).addBinding()
                    .to(AWSS3SecurityMappingConfigurationProvider.class).in(Scopes.SINGLETON);
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

    public static class HadoopDefaultConfigurationUpdater
            implements S3ConfigurationUpdater
    {
        @Override
        public void updateConfiguration(Configuration config)
        {
        }
    }
}
