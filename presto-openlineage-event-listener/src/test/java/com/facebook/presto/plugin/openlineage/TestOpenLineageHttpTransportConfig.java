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
package com.facebook.presto.plugin.openlineage;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenLineageHttpTransportConfig
{
    @Test
    public void testDefaults()
    {
        OpenLineageHttpTransportConfig config = new OpenLineageHttpTransportConfig();
        assertThat(config.getUrl()).isNull();
        assertThat(config.getEndpoint()).isNull();
        assertThat(config.getTimeoutMillis()).isEqualTo(5000L);
        assertThat(config.getApiKey()).isEmpty();
        assertThat(config.getHeaders()).isEmpty();
        assertThat(config.getUrlParams()).isEmpty();
        assertThat(config.getCompression()).isEqualTo(OpenLineageHttpTransportConfig.Compression.NONE);
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openlineage-event-listener.transport.url", "http://testurl")
                .put("openlineage-event-listener.transport.endpoint", "/test/endpoint")
                .put("openlineage-event-listener.transport.api-key", "dummy")
                .put("openlineage-event-listener.transport.timeout", "30s")
                .put("openlineage-event-listener.transport.headers", "header1:value1,header2:value2")
                .put("openlineage-event-listener.transport.url-params", "urlParam1:urlVal1,urlParam2:urlVal2")
                .put("openlineage-event-listener.transport.compression", "gzip")
                .build();

        OpenLineageHttpTransportConfig config = new OpenLineageHttpTransportConfig(properties);

        assertThat(config.getUrl()).isEqualTo(URI.create("http://testurl"));
        assertThat(config.getEndpoint()).isEqualTo("/test/endpoint");
        assertThat(config.getApiKey()).hasValue("dummy");
        assertThat(config.getTimeoutMillis()).isEqualTo(30000L);
        assertThat(config.getHeaders()).containsEntry("header1", "value1").containsEntry("header2", "value2");
        assertThat(config.getUrlParams()).containsEntry("urlParam1", "urlVal1").containsEntry("urlParam2", "urlVal2");
        assertThat(config.getCompression()).isEqualTo(OpenLineageHttpTransportConfig.Compression.GZIP);
    }
}
