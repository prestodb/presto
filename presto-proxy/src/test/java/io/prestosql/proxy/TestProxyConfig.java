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
package io.prestosql.proxy;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestProxyConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ProxyConfig.class)
                .setUri(null)
                .setSharedSecretFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("proxy.uri", "http://example.net/")
                .put("proxy.shared-secret-file", "test.secret")
                .build();

        ProxyConfig expected = new ProxyConfig()
                .setUri(URI.create("http://example.net/"))
                .setSharedSecretFile(new File("test.secret"));

        assertFullMapping(properties, expected);
    }
}
