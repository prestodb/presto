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

import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAWSSecurityMappingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AWSSecurityMappingConfig.class)
                .setMappingType(null)
                .setConfigFile(null)
                .setRefreshPeriod(Duration.valueOf("30s")));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path securityMappingConfigFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.aws.security-mapping.type", "S3")
                .put("hive.aws.security-mapping.config-file", securityMappingConfigFile.toString())
                .put("hive.aws.security-mapping.refresh-period", "60s")
                .build();

        AWSSecurityMappingConfig expected = new AWSSecurityMappingConfig()
                .setMappingType(AWSSecurityMappingType.S3)
                .setConfigFile(securityMappingConfigFile.toFile())
                .setRefreshPeriod(Duration.valueOf("60s"));

        assertFullMapping(properties, expected);
    }
}
