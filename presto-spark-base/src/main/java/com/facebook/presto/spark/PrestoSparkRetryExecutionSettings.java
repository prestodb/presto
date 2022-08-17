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

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PrestoSparkRetryExecutionSettings
{
    private final Map<String, String> sparkSettings;
    private final Map<String, String> prestoSettings;

    public PrestoSparkRetryExecutionSettings(Map<String, String> sparkSettings, Map<String, String> prestoSettings)
    {
        this.sparkSettings = requireNonNull(sparkSettings, "sparkSettings is null");
        this.prestoSettings = requireNonNull(prestoSettings, "sparkSettings is null");
    }

    public Map<String, String> getSparkSettings()
    {
        return sparkSettings;
    }

    public Map<String, String> getPrestoSettings()
    {
        return prestoSettings;
    }
}
