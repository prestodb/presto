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
package com.facebook.presto.elasticsearch;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestElasticsearchConnectorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(ElasticsearchConnectorConfig.class)
                .setTableDescriptionDir(new File("etc/elasticsearch/"))
                .setDefaultSchema("default")
                .setTableNames("")
                .setScrollSize(1000)
                .setScrollTimeout(new Duration(1, SECONDS))
                .setMaxHits(1000000)
                .setRequestTimeout(new Duration(100, MILLISECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("elasticsearch.table-description-dir", "/etc/elasticsearch/")
                .put("elasticsearch.table-names", "test.table1,test.table2,test.table3")
                .put("elasticsearch.default-schema", "test")
                .put("elasticsearch.scroll-size", "4000")
                .put("elasticsearch.scroll-timeout", "20s")
                .put("elasticsearch.max-hits", "20000")
                .put("elasticsearch.request-timeout", "1s")
                .build();

        ElasticsearchConnectorConfig expected = new ElasticsearchConnectorConfig()
                .setTableDescriptionDir(new File("/etc/elasticsearch/"))
                .setTableNames("test.table1,test.table2,test.table3")
                .setDefaultSchema("test")
                .setScrollSize(4000)
                .setScrollTimeout(new Duration(20, SECONDS))
                .setMaxHits(20000)
                .setRequestTimeout(new Duration(1, SECONDS));

        assertFullMapping(properties, expected);
    }
}
