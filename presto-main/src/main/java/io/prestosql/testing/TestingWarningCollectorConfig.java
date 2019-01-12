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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import static com.google.common.base.Preconditions.checkArgument;

public class TestingWarningCollectorConfig
{
    private int preloadedWarnings;
    private boolean addWarnings;

    @Config("testing-warning-collector.preloaded-warnings")
    @ConfigDescription("Preloads warning collector with test warnings")
    public TestingWarningCollectorConfig setPreloadedWarnings(int preloadedWarnings)
    {
        checkArgument(preloadedWarnings >= 0, "preloadedWarnings must be >= 0");
        this.preloadedWarnings = preloadedWarnings;
        return this;
    }

    public int getPreloadedWarnings()
    {
        return preloadedWarnings;
    }

    @Config("testing-warning-collector.add-warnings")
    @ConfigDescription("Adds a warning each time getWarnings is called")
    public TestingWarningCollectorConfig setAddWarnings(boolean addWarnings)
    {
        this.addWarnings = addWarnings;
        return this;
    }

    public boolean getAddWarnings()
    {
        return addWarnings;
    }
}
