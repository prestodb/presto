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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Json description to parse a row on a Kafka topic. A row contains a message and an optional key. See the documentation for the exact JSON syntax.
 */
public class KafkaTopicDescription
{
    private final String tableName;
    private final String topicName;
    private final String schemaName;
    private final KafkaTopicFieldGroup key;
    private final KafkaTopicFieldGroup message;

    @JsonCreator
    public KafkaTopicDescription(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("key") KafkaTopicFieldGroup key,
            @JsonProperty("message") KafkaTopicFieldGroup message)
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        this.tableName = tableName;
        this.topicName = checkNotNull(topicName, "topicName is null");
        this.schemaName = schemaName;
        this.key = key;
        this.message = message;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getTopicName()
    {
        return topicName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public KafkaTopicFieldGroup getKey()
    {
        return key;
    }

    @JsonProperty
    public KafkaTopicFieldGroup getMessage()
    {
        return message;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("topicName", topicName)
                .add("schemaName", schemaName)
                .add("key", key)
                .add("message", message)
                .toString();
    }
}
