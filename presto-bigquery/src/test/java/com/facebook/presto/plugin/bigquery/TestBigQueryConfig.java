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
package com.facebook.presto.plugin.bigquery;

import com.facebook.airlift.configuration.ConfigurationFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static org.testng.Assert.assertEquals;

public class TestBigQueryConfig
{
    @Test
    public void testDefaults()
    {
        BigQueryConfig config = new BigQueryConfig()
                .setCredentialsKey("ckey")
                .setCredentialsFile("cfile")
                .setProjectId("pid")
                .setParentProjectId("ppid")
                .setParallelism(20)
                .setViewMaterializationProject("vmproject")
                .setViewMaterializationDataset("vmdataset")
                .setMaxReadRowsRetries(10)
                .setCaseSensitiveNameMatching(false);

        assertEquals(config.getCredentialsKey(), Optional.of("ckey"));
        assertEquals(config.getCredentialsFile(), Optional.of("cfile"));
        assertEquals(config.getProjectId(), Optional.of("pid"));
        assertEquals(config.getParentProjectId(), Optional.of("ppid"));
        assertEquals(config.getParallelism(), OptionalInt.of(20));
        assertEquals(config.getViewMaterializationProject(), Optional.of("vmproject"));
        assertEquals(config.getViewMaterializationDataset(), Optional.of("vmdataset"));
        assertEquals(config.getMaxReadRowsRetries(), 10);
        assertEquals(config.isCaseSensitiveNameMatching(), false);
    }

    @Test
    public void testExplicitPropertyMappingsWithCredentialsKey()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("bigquery.credentials-key", "ckey")
                .put("bigquery.project-id", "pid")
                .put("bigquery.parent-project-id", "ppid")
                .put("bigquery.parallelism", "20")
                .put("bigquery.view-materialization-project", "vmproject")
                .put("bigquery.view-materialization-dataset", "vmdataset")
                .put("bigquery.max-read-rows-retries", "10")
                .put("case-sensitive-name-matching", "true")
                .build();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        BigQueryConfig config = configurationFactory.build(BigQueryConfig.class);

        assertEquals(config.getCredentialsKey(), Optional.of("ckey"));
        assertEquals(config.getProjectId(), Optional.of("pid"));
        assertEquals(config.getParentProjectId(), Optional.of("ppid"));
        assertEquals(config.getParallelism(), OptionalInt.of(20));
        assertEquals(config.getViewMaterializationProject(), Optional.of("vmproject"));
        assertEquals(config.getViewMaterializationDataset(), Optional.of("vmdataset"));
        assertEquals(config.getMaxReadRowsRetries(), 10);
        assertEquals(config.isCaseSensitiveNameMatching(), true);
    }

    @Test
    public void testExplicitPropertyMappingsWithCredentialsFile()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("bigquery.credentials-file", "cfile")
                .build();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        BigQueryConfig config = configurationFactory.build(BigQueryConfig.class);

        assertEquals(config.getCredentialsFile(), Optional.of("cfile"));
    }
}
