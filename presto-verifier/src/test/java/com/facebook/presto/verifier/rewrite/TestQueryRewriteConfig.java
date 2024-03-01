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
package com.facebook.presto.verifier.rewrite;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.testng.Assert.assertEquals;

public class TestQueryRewriteConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(QueryRewriteConfig.class)
                .setTablePrefix("tmp_verifier")
                .setTableProperties(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("table-prefix", "local.tmp")
                .put("table-properties", "{\"retention\":21}")
                .build();
        QueryRewriteConfig expected = new QueryRewriteConfig()
                .setTablePrefix("local.tmp")
                .setTableProperties("{\"retention\":21}");

        assertFullMapping(properties, expected);
        assertEquals(expected.getTableProperties(), ImmutableMap.of("retention", 21));
    }
}
