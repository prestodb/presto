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
package com.facebook.presto.plugin.ranger.security.ranger;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class RangerBasedAccessControlConfig
{
    private String configFile;
    private String keyTab;
    private String principle;

    @NotNull
    public String getConfigFile()
    {
        return configFile;
    }

    @NotNull
    public String getKeyTab()
    {
        return keyTab;
    }

    @NotNull
    public String getPrinciple()
    {
        return principle;
    }

    @Config("security.ranger.config-file")
    public RangerBasedAccessControlConfig setConfigFile(String configFile)
    {
        this.configFile = configFile;
        return this;
    }

    @Config("security.ranger.keytab")
    public RangerBasedAccessControlConfig setKeyTab(String configFile)
    {
        this.keyTab = configFile;
        return this;
    }

    @Config("security.ranger.principle")
    public RangerBasedAccessControlConfig setPrinciple(String configFile)
    {
        this.principle = configFile;
        return this;
    }
}
