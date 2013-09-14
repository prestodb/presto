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
package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Map;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

public class TestDatabaseLocalStorageManagerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(DatabaseLocalStorageManagerConfig.class)
                .setDataDirectory(new File("var/data"))
                .setTasksPerNode(32));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("storage-manager.data-directory", "/data")
                .put("storage-manager.tasks-per-node", "16")
                .build();

        DatabaseLocalStorageManagerConfig expected = new DatabaseLocalStorageManagerConfig()
                .setDataDirectory(new File("/data"))
                .setTasksPerNode(16);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testValidations()
    {
        assertFailsValidation(new DatabaseLocalStorageManagerConfig().setDataDirectory(null), "dataDirectory", "may not be null", NotNull.class);
    }
}
