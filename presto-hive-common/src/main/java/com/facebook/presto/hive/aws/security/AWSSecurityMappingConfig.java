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
package com.facebook.presto.hive.aws.security;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.AssertTrue;

import java.io.File;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AWSSecurityMappingConfig
{
    private static final String MAPPING_TYPE = "hive.aws.security-mapping.type";
    private static final String CONFIG_FILE = "hive.aws.security-mapping.config-file";
    private static final String REFRESH_PERIOD = "hive.aws.security-mapping.refresh-period";
    private AWSSecurityMappingType mappingType;
    private File configFile;
    private Duration refreshPeriod = new Duration(30, SECONDS);

    public AWSSecurityMappingType getMappingType()
    {
        return mappingType;
    }

    @Config(MAPPING_TYPE)
    @ConfigDescription("AWS Security Mapping Type. Possible values: S3 or LAKEFORMATION")
    public AWSSecurityMappingConfig setMappingType(AWSSecurityMappingType mappingType)
    {
        this.mappingType = mappingType;
        return this;
    }

    public Optional<File> getConfigFile()
    {
        return Optional.ofNullable(configFile);
    }

    @Nullable
    @Config(CONFIG_FILE)
    @ConfigDescription("JSON configuration file containing AWS IAM Security mappings")
    public AWSSecurityMappingConfig setConfigFile(File configFile)
    {
        this.configFile = configFile;
        return this;
    }

    public Duration getRefreshPeriod()
    {
        return refreshPeriod;
    }

    @MinDuration("0ms")
    @Config(REFRESH_PERIOD)
    @ConfigDescription("Time interval after which AWS IAM security mapping configuration will be refreshed")
    public AWSSecurityMappingConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = requireNonNull(refreshPeriod, "refreshPeriod is null");
        return this;
    }

    @AssertTrue(message = "MAPPING TYPE(" + MAPPING_TYPE + ") must be configured when AWS Security Mapping Config File(" + CONFIG_FILE + ") is set and vice versa")
    public boolean isValidConfiguration()
    {
        return (getConfigFile().isPresent() && getMappingType() != null) || (!getConfigFile().isPresent() && getMappingType() == null);
    }
}
