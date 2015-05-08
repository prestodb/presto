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
package com.facebook.presto.kinesis;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 *  This Class maintains all the details of Kinesis stream like name, fields of data, Presto table stream is mapping to, tables's schema name
 *
 */
public class KinesisStreamDescription
{
    private final String tableName;
    private final String streamName;
    private final String schemaName;
    private final KinesisStreamFieldGroup message;

    @JsonCreator
    public KinesisStreamDescription(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("streamName") String streamName,
            @JsonProperty("message") KinesisStreamFieldGroup message)
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        this.tableName = tableName;
        this.streamName = checkNotNull(streamName, "topicName is null");
        this.schemaName = schemaName;
        this.message = message;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getStreamName()
    {
        return streamName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public KinesisStreamFieldGroup getMessage()
    {
        return message;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("streamName", streamName)
                .add("schemaName", schemaName)
                .add("message", message)
                .toString();
    }
}
