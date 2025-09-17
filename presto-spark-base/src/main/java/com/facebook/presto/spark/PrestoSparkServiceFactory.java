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
package com.facebook.presto.spark;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkService;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkServiceFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkBootstrapTimer;
import com.facebook.presto.spark.classloader_interface.PrestoSparkConfiguration;
import com.facebook.presto.spark.classloader_interface.SparkProcessType;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionModule;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spark.classloader_interface.PrestoSparkConfiguration.METADATA_STORAGE_TYPE_LOCAL;
import static com.google.common.base.Preconditions.checkArgument;

public class PrestoSparkServiceFactory
        implements IPrestoSparkServiceFactory
{
    private final Logger log = Logger.get(PrestoSparkServiceFactory.class);

    @Override
    public IPrestoSparkService createService(SparkProcessType sparkProcessType, PrestoSparkConfiguration configuration, PrestoSparkBootstrapTimer bootstrapTimer)
    {
        bootstrapTimer.beginPrestoSparkServiceCreation();
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.putAll(configuration.getConfigProperties());
        properties.put("plugin.dir", configuration.getPluginsDirectoryPath());

        PrestoSparkInjectorFactory prestoSparkInjectorFactory = new PrestoSparkInjectorFactory(
                sparkProcessType,
                properties.build(),
                configuration.getCatalogProperties(),
                configuration.getNativeWorkerConfigProperties(),
                configuration.getEventListenerProperties(),
                configuration.getAccessControlProperties(),
                configuration.getSessionPropertyConfigurationProperties(),
                configuration.getFunctionNamespaceProperties(),
                configuration.getTempStorageProperties(),
                getSqlParserOptions(),
                getAdditionalModules(configuration));

        Injector injector = prestoSparkInjectorFactory.create(bootstrapTimer);
        PrestoSparkService prestoSparkService = injector.getInstance(PrestoSparkService.class);
        bootstrapTimer.endPrestoSparkServiceCreation();
        log.info("Initialized");
        return prestoSparkService;
    }

    protected List<Module> getAdditionalModules(PrestoSparkConfiguration configuration)
    {
        checkArgument(METADATA_STORAGE_TYPE_LOCAL.equalsIgnoreCase(configuration.getMetadataStorageType()), "only local metadata storage is supported");
        return ImmutableList.of(
                new PrestoSparkLocalMetadataStorageModule(),
                // TODO: Need to let NativeExecutionModule addition be controlled by configuration as well.
                new NativeExecutionModule(Optional.of(configuration.getCatalogProperties())));
    }

    protected SqlParserOptions getSqlParserOptions()
    {
        return new SqlParserOptions();
    }
}
