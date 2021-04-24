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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

public final class KafkaSessionProperties
{
    private static final String TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED = "timestamp_upper_bound_force_push_down_enabled";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public KafkaSessionProperties(KafkaConnectorConfig kafkaConfig)
    {
        sessionProperties = ImmutableList.of(PropertyMetadata.booleanProperty(
                TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED,
                "Enable or disable timestamp upper bound push down for topic createTime mode",
                kafkaConfig.isTimestampUpperBoundPushDownEnabled(), false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    /**
     * If predicate specifies lower bound on _timestamp column (_timestamp > XXXX), it is always pushed down.
     * The upper bound predicate is pushed down only for topics using ``LogAppendTime`` mode.
     * For topics using ``CreateTime`` mode, upper bound push down must be explicitly
     *  allowed via ``kafka.timestamp-upper-bound-force-push-down-enabled`` config property
     *  or ``timestamp_upper_bound_force_push_down_enabled`` session property.
     */
    public static boolean isTimestampUpperBoundPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED, Boolean.class);
    }
}
