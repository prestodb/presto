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
package com.facebook.presto.hive;

import com.facebook.presto.hadoop.shaded.com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.transform;
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
                .setMetastoreUris(null));
    }

    @Test
    public void testExplicitPropertyMappingsSingleMetastore()
    {
        String metastoreUriString = "thrift://localhost:9083";
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.uri", metastoreUriString)
                .build();

        StaticMetastoreConfig expected = new StaticMetastoreConfig()
                .setMetastoreUris(metastoreUriString);

        assertFullMapping(properties, expected);
        assertEquals(expected.getMetastoreUris().get(0), URI.create(metastoreUriString));
    }

    @Test
    public void testExplicitPropertyMappingsMultipleMetastores()
    {
        List<String> metastoreUriStrings = Arrays.asList("thrift://localhost:9083", "thrift://192.10.10.3:8932");
        Joiner commaJoiner = Joiner.on(",");
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.uri", commaJoiner.join(metastoreUriStrings))
                .build();

        StaticMetastoreConfig expected = new StaticMetastoreConfig()
                .setMetastoreUris(commaJoiner.join(metastoreUriStrings));

        assertFullMapping(properties, expected);
        assertEquals(expected.getMetastoreUris(), transform(metastoreUriStrings, URI::create));
    }
}
