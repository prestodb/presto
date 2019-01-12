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
package io.prestosql.testing;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.prestosql.execution.warnings.WarningCollectorConfig;
import io.prestosql.execution.warnings.WarningCollectorFactory;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class TestingWarningCollectorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(WarningCollectorConfig.class);
        configBinder(binder).bindConfig(TestingWarningCollectorConfig.class);
    }

    @Provides
    @Singleton
    public WarningCollectorFactory createWarningCollectorFactory(WarningCollectorConfig config, TestingWarningCollectorConfig testConfig)
    {
        requireNonNull(config, "config is null");
        requireNonNull(testConfig, "testConfig is null");
        return () -> new TestingWarningCollector(config, testConfig);
    }
}
