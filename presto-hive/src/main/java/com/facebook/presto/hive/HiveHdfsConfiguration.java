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

import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class HiveHdfsConfiguration
        implements HdfsConfiguration
{
    private static final Configuration DEFAULT_CONFIGURATION = new Configuration();

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<Configuration> hadoopConfiguration = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            Configuration config = new Configuration(false);
            for (Map.Entry<String, String> entry : DEFAULT_CONFIGURATION) {
                config.set(entry.getKey(), entry.getValue());
            }
            updater.updateConfiguration(config);
            return config;
        }
    };

    private final HdfsConfigurationUpdater updater;

    @Inject
    public HiveHdfsConfiguration(HdfsConfigurationUpdater updater)
    {
        this.updater = requireNonNull(updater, "updater is null");
    }

    @Override
    public Configuration getConfiguration(URI uri)
    {
        // use the same configuration for everything
        return hadoopConfiguration.get();
    }
}
