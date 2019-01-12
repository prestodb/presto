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
package io.prestosql.plugin.localfile;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

public class TestLocalFileConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(LocalFileConfig.class)
                .setHttpRequestLogLocation("var/log/http-request.log")
                .setHttpRequestLogFileNamePattern(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("presto-logs.http-request-log.location", "/data/foo.log")
                .put("presto-logs.http-request-log.pattern", "bar")
                .build();

        LocalFileConfig expected = new LocalFileConfig()
                .setHttpRequestLogLocation("/data/foo.log")
                .setHttpRequestLogFileNamePattern("bar");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
