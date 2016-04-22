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
package com.facebook.presto.mongodb;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MongoConnectorId
{
    private final String connectorId;

    public MongoConnectorId(String connectorId)
    {
        requireNonNull(connectorId, "connectorId is null");
        checkArgument(!connectorId.isEmpty(), "connectorId is empty");
        this.connectorId = connectorId;
    }

    @Override
    public int hashCode()
    {
        return connectorId.hashCode();
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
        MongoConnectorId other = (MongoConnectorId) obj;
        return this.connectorId.equals(other.connectorId);
    }

    @Override
    public String toString()
    {
        return connectorId;
    }
}
