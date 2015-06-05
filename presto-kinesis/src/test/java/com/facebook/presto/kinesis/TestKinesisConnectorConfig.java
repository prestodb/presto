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
package com.facebook.presto.kinesis;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import io.airlift.configuration.testing.ConfigAssertions;

import java.io.File;
import java.util.Map;

public class TestKinesisConnectorConfig
{
    @Parameters({
        "kinesis.awsAccessKey",
        "kinesis.awsSecretKey"
    })
    @Test
    public void testDefaults(String accessKey, String secretKey)
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(KinesisConnectorConfig.class)
                .setDefaultSchema("default")
                .setHideInternalColumns(true)
                .setTableNames("")
                .setTableDescriptionDir(new File("etc/kinesis/"))
                .setAccessKey(null)
                .setSecretKey(null));
    }

    @Parameters({
        "kinesis.awsAccessKey",
        "kinesis.awsSecretKey"
    })
    @Test
    public void testExplicitPropertyMappings(String accessKey, String secretKey)
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kinesis.table-description-dir", "/var/lib/kinesis")
                .put("kinesis.table-names", "table1, table2, table3")
                .put("kinesis.default-schema", "kinesis")
                .put("kinesis.hide-internal-columns", "false")
                .put("kinesis.access-key", accessKey)
                .put("kinesis.secret-key", secretKey)
                .build();

        KinesisConnectorConfig expected = new KinesisConnectorConfig()
                .setTableDescriptionDir(new File("/var/lib/kinesis"))
                .setTableNames("table1, table2, table3")
                .setDefaultSchema("kinesis")
                .setHideInternalColumns(false)
                .setAccessKey(accessKey)
                .setSecretKey(secretKey);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
