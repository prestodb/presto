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
import io.openlineage.client.OpenLineage.RunEvent;
import org.testng.annotations.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class TestOpenLineageEventListener
{
    @Test
    public void testGetCompleteEvent()
    {
        OpenLineageEventListener listener = (OpenLineageEventListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.presto.uri", "http://testhost"));

        RunEvent result = listener.getCompletedEvent(PrestoEventData.queryCompleteEvent);

        assertThat(result.getEventType()).isEqualTo(RunEvent.EventType.COMPLETE);
        assertThat(result.getEventTime().toInstant()).isEqualTo(PrestoEventData.queryCompleteEvent.getEndTime());
        assertThat(result.getRun().getRunId().toString()).startsWith("01967c23-ae78-7");
        assertThat(result.getJob().getNamespace()).isEqualTo("presto://testhost");
        assertThat(result.getJob().getName()).isEqualTo("queryId");

        Map<String, Object> prestoQueryMetadata = result
                .getRun()
                .getFacets()
                .getAdditionalProperties()
                .get("presto_metadata")
                .getAdditionalProperties();

        assertThat(prestoQueryMetadata)
                .containsOnly(
                        entry("query_id", "queryId"),
                        entry("transaction_id", "transactionId"),
                        entry("query_plan", "queryPlan"));

        Map<String, Object> prestoQueryContext =
                result
                        .getRun()
                        .getFacets()
                        .getAdditionalProperties()
                        .get("presto_query_context")
                        .getAdditionalProperties();

        assertThat(prestoQueryContext)
                .containsOnly(
                        entry("server_address", "serverAddress"),
                        entry("environment", "environment"),
                        entry("user", "user"),
                        entry("principal", "principal"),
                        entry("source", "some-presto-client"),
                        entry("client_info", "Some client info"),
                        entry("remote_client_address", "127.0.0.1"),
                        entry("user_agent", "Some-User-Agent"));
    }

    @Test
    public void testGetStartEvent()
    {
        OpenLineageEventListener listener = (OpenLineageEventListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", OpenLineageTransport.CONSOLE.toString(),
                "openlineage-event-listener.presto.uri", "http://testhost:8080"));

        RunEvent result = listener.getStartEvent(PrestoEventData.queryCreatedEvent);

        assertThat(result.getEventType()).isEqualTo(RunEvent.EventType.START);
        assertThat(result.getEventTime().toInstant()).isEqualTo(PrestoEventData.queryCreatedEvent.getCreateTime());
        assertThat(result.getRun().getRunId().toString()).startsWith("01967c23-ae78-7");
        assertThat(result.getJob().getNamespace()).isEqualTo("presto://testhost:8080");
        assertThat(result.getJob().getName()).isEqualTo("queryId");
    }

    @Test
    public void testJobNameFormatting()
    {
        OpenLineageEventListener listener = (OpenLineageEventListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.presto.uri", "http://testhost:8080",
                "openlineage-event-listener.job.name-format", "$QUERY_ID-$USER-$SOURCE-$CLIENT_IP-abc123"));

        RunEvent result = listener.getCompletedEvent(PrestoEventData.queryCompleteEvent);

        assertThat(result.getJob().getNamespace()).isEqualTo("presto://testhost:8080");
        assertThat(result.getJob().getName()).isEqualTo("queryId-user-some-presto-client-127.0.0.1-abc123");
    }

    private static com.facebook.presto.spi.eventlistener.EventListener createEventListener(Map<String, String> config)
    {
        return new OpenLineageEventListenerFactory().create(ImmutableMap.copyOf(config));
    }
}
