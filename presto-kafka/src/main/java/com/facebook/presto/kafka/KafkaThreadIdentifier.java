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

import com.facebook.presto.spi.HostAddress;

import java.util.Objects;

public class KafkaThreadIdentifier
{
    public final String threadId;
    public final HostAddress hostAddress;
    public final String partitionId;

    public KafkaThreadIdentifier(
            String partitionId,
            Long threadId,
            HostAddress hostAddress)
    {
        this.threadId = Long.toString(threadId);
        this.hostAddress = hostAddress;
        this.partitionId = partitionId;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(threadId.length());
        builder.append(String.format("%s", threadId));
        return builder.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final KafkaThreadIdentifier other = (KafkaThreadIdentifier) obj;
        return Objects.equals(this.threadId, other.threadId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(threadId);
    }
}
