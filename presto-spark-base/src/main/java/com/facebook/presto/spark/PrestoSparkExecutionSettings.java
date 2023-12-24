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

public class PrestoSparkExecutionSettings
{
    private final Map<String, String> sparkConfigProperties;
    private final Map<String, String> prestoSessionProperties;

    public PrestoSparkExecutionSettings(
            Map<String, String> sparkConfigProperties,
            Map<String, String> prestoSessionProperties)
    {
        this.sparkConfigProperties = requireNonNull(sparkConfigProperties, "sparkConfigProperties is null");
        this.prestoSessionProperties = requireNonNull(prestoSessionProperties, "prestoSessionProperties is null");
    }

    public Map<String, String> getSparkConfigProperties()
    {
        return sparkConfigProperties;
    }

    public Map<String, String> getPrestoSessionProperties()
    {
        return prestoSessionProperties;
    }
}
