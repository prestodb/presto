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

package com.facebook.presto.operator;

import com.facebook.presto.operator.PartitionedOutputOperator.PartitionedOutputInfo;
import com.facebook.presto.operator.exchange.LocalExchangeBufferInfo;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ExchangeClientStatus.class, name = "exchangeClientStatus"),
        @JsonSubTypes.Type(value = LocalExchangeBufferInfo.class, name = "localExchangeBuffer"),
        @JsonSubTypes.Type(value = TableFinishInfo.class, name = "tableFinish"),
        @JsonSubTypes.Type(value = SplitOperatorInfo.class, name = "splitOperator"),
        @JsonSubTypes.Type(value = HashCollisionsInfo.class, name = "hashCollisionsInfo"),
        @JsonSubTypes.Type(value = PartitionedOutputInfo.class, name = "partitionedOutput")
})
public interface OperatorInfo
{
    /**
     * @return true if this OperatorInfo should be collected and sent to the coordinator when the task completes (i.e. it will not be stripped away during summarization).
     */
    default boolean isFinal()
    {
        return false;
    }
}
