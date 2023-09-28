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
package com.facebook.presto.features.http;

import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonProperty;

@ThriftStruct
public class FeatureDetails
{
    private final FeatureInfo initialConfiguration;
    private final FeatureInfo configurationOverride;
    private final FeatureInfo activeConfiguration;

    public FeatureDetails(FeatureInfo initialConfiguration, FeatureInfo configurationOverride, FeatureInfo activeConfiguration)
    {
        this.initialConfiguration = initialConfiguration;
        this.configurationOverride = configurationOverride;
        this.activeConfiguration = activeConfiguration;
    }

    @JsonProperty
    @ThriftField(1)
    public FeatureInfo getInitialConfiguration()
    {
        return initialConfiguration;
    }

    @JsonProperty
    @ThriftField(2)
    public FeatureInfo getConfigurationOverride()
    {
        return configurationOverride;
    }

    @JsonProperty
    @ThriftField(3)
    public FeatureInfo getActiveConfiguration()
    {
        return activeConfiguration;
    }
}
