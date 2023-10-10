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
package com.facebook.presto.hive.aws.lakeformation;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import java.io.File;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LakeFormationSecurityMappingConfig
{
    private File configFile;
    private Duration refreshPeriod = new Duration(30, SECONDS);

    public Optional<File> getConfigFile()
    {
        if (configFile != null) {
            checkArgument(configFile.exists() && configFile.isFile(), "AWS Lake Formation Security Mapping config file does not exist: %s", configFile);
        }
        return Optional.ofNullable(configFile);
    }

    @Config("hive.metastore.glue.lakeformation.security-mapping.config-file")
    @ConfigDescription("JSON configuration file containing AWS Lake Formation security mappings")
    public LakeFormationSecurityMappingConfig setConfigFile(File configFile)
    {
        this.configFile = configFile;
        return this;
    }

    public Optional<Duration> getRefreshPeriod()
    {
        return Optional.ofNullable(refreshPeriod);
    }

    @MinDuration("0ms")
    @Config("hive.metastore.glue.lakeformation.security-mapping.refresh-period")
    @ConfigDescription("Time interval after which securing mapping configuration will be refreshed")
    public LakeFormationSecurityMappingConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }
}
