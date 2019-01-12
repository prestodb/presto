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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.testng.Assert.assertEquals;

public class TestStaticMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StaticMetastoreConfig.class)
                .setMetastoreUris(null)
                .setMetastoreUsername(null));
    }

    @Test
    public void testExplicitPropertyMappingsSingleMetastore()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.uri", "thrift://localhost:9083")
                .put("hive.metastore.username", "presto")
                .build();

        StaticMetastoreConfig expected = new StaticMetastoreConfig()
                .setMetastoreUris("thrift://localhost:9083")
                .setMetastoreUsername("presto");

        assertFullMapping(properties, expected);
        assertEquals(expected.getMetastoreUris(), ImmutableList.of(URI.create("thrift://localhost:9083")));
        assertEquals(expected.getMetastoreUsername(), "presto");
    }

    @Test
    public void testExplicitPropertyMappingsMultipleMetastores()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.uri", "thrift://localhost:9083,thrift://192.0.2.3:8932")
                .put("hive.metastore.username", "presto")
                .build();

        StaticMetastoreConfig expected = new StaticMetastoreConfig()
                .setMetastoreUris("thrift://localhost:9083,thrift://192.0.2.3:8932")
                .setMetastoreUsername("presto");

        assertFullMapping(properties, expected);
        assertEquals(expected.getMetastoreUris(), ImmutableList.of(
                URI.create("thrift://localhost:9083"),
                URI.create("thrift://192.0.2.3:8932")));
        assertEquals(expected.getMetastoreUsername(), "presto");
    }
}
