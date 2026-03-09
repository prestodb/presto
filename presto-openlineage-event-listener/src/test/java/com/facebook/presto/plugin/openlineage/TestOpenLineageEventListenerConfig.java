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

import static com.facebook.presto.common.resourceGroups.QueryType.DELETE;
import static com.facebook.presto.common.resourceGroups.QueryType.INSERT;
import static com.facebook.presto.common.resourceGroups.QueryType.SELECT;
import static com.facebook.presto.plugin.openlineage.OpenLineagePrestoFacet.PRESTO_METADATA;
import static com.facebook.presto.plugin.openlineage.OpenLineagePrestoFacet.PRESTO_QUERY_STATISTICS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenLineageEventListenerConfig
{
    @Test
    public void testDefaults()
    {
        OpenLineageEventListenerConfig config = new OpenLineageEventListenerConfig();
        assertThat(config.getPrestoURI()).isNull();
        assertThat(config.getNamespace()).isEmpty();
        assertThat(config.getJobNameFormat()).isEqualTo("$QUERY_ID");
        assertThat(config.getDisabledFacets()).isEmpty();
        assertThat(config.getIncludeQueryTypes()).containsExactlyInAnyOrder(
                DELETE, INSERT,
                com.facebook.presto.common.resourceGroups.QueryType.MERGE,
                com.facebook.presto.common.resourceGroups.QueryType.UPDATE,
                com.facebook.presto.common.resourceGroups.QueryType.DATA_DEFINITION);
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openlineage-event-listener.presto.uri", "http://testpresto")
                .put("openlineage-event-listener.presto.include-query-types", "SELECT,DELETE")
                .put("openlineage-event-listener.disabled-facets", "PRESTO_METADATA,PRESTO_QUERY_STATISTICS")
                .put("openlineage-event-listener.namespace", "testnamespace")
                .put("openlineage-event-listener.job.name-format", "$QUERY_ID-$USER-$SOURCE-$CLIENT_IP-abc123")
                .build();

        OpenLineageEventListenerConfig config = new OpenLineageEventListenerConfig(properties);

        assertThat(config.getPrestoURI()).isEqualTo(URI.create("http://testpresto"));
        assertThat(config.getIncludeQueryTypes()).containsExactlyInAnyOrder(SELECT, DELETE);
        assertThat(config.getDisabledFacets()).containsExactlyInAnyOrder(PRESTO_METADATA, PRESTO_QUERY_STATISTICS);
        assertThat(config.getNamespace()).hasValue("testnamespace");
        assertThat(config.getJobNameFormat()).isEqualTo("$QUERY_ID-$USER-$SOURCE-$CLIENT_IP-abc123");
    }

    @Test
    public void testIsJobNameFormatValid()
    {
        assertThat(configWithFormat("abc123").isJobNameFormatValid()).isTrue();
        assertThat(configWithFormat("$QUERY_ID").isJobNameFormatValid()).isTrue();
        assertThat(configWithFormat("$USER").isJobNameFormatValid()).isTrue();
        assertThat(configWithFormat("$SOURCE").isJobNameFormatValid()).isTrue();
        assertThat(configWithFormat("$CLIENT_IP").isJobNameFormatValid()).isTrue();
        assertThat(configWithFormat("$QUERY_ID-$USER-$SOURCE-$CLIENT_IP-abc123").isJobNameFormatValid()).isTrue();
        assertThat(configWithFormat("$QUERY_ID $USER $SOURCE $CLIENT_IP abc123").isJobNameFormatValid()).isTrue();

        assertThat(configWithFormat("$query_id").isJobNameFormatValid()).isFalse();
        assertThat(configWithFormat("$UNKNOWN").isJobNameFormatValid()).isFalse();
        assertThat(configWithFormat("${QUERY_ID}").isJobNameFormatValid()).isFalse();
        assertThat(configWithFormat("$$QUERY_ID").isJobNameFormatValid()).isFalse();
        assertThat(configWithFormat("\\$QUERY_ID").isJobNameFormatValid()).isFalse();
    }

    private static OpenLineageEventListenerConfig configWithFormat(String format)
    {
        return new OpenLineageEventListenerConfig()
                .setPrestoURI(URI.create("http://testpresto"))
                .setJobNameFormat(format);
    }
}
