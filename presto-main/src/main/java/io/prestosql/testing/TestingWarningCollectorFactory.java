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

import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.execution.warnings.WarningCollectorConfig;
import io.prestosql.execution.warnings.WarningCollectorFactory;

import static java.util.Objects.requireNonNull;

public class TestingWarningCollectorFactory
        implements WarningCollectorFactory
{
    private final WarningCollectorConfig config;
    private final TestingWarningCollectorConfig testConfig;

    public TestingWarningCollectorFactory(WarningCollectorConfig config, TestingWarningCollectorConfig testConfig)
    {
        this.config = requireNonNull(config, "config is null");
        this.testConfig = requireNonNull(testConfig, "testConfig is null");
    }

    @Override
    public WarningCollector create()
    {
        return new TestingWarningCollector(config, testConfig);
    }
}
