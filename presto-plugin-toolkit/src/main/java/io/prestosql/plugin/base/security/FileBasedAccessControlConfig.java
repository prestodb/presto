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
package io.prestosql.plugin.base.security;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

public class FileBasedAccessControlConfig
{
    public static final String SECURITY_CONFIG_FILE = "security.config-file";
    public static final String SECURITY_REFRESH_PERIOD = "security.refresh-period";

    private String configFile;
    private Duration refreshPeriod;

    @NotNull
    public String getConfigFile()
    {
        return configFile;
    }

    @Config(SECURITY_CONFIG_FILE)
    public FileBasedAccessControlConfig setConfigFile(String configFile)
    {
        this.configFile = configFile;
        return this;
    }

    @MinDuration("1ms")
    public Duration getRefreshPeriod()
    {
        return refreshPeriod;
    }

    @Config(SECURITY_REFRESH_PERIOD)
    public FileBasedAccessControlConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }
}
