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
package com.facebook.presto.tablestore;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class TablestoreTransactionHandle
        implements ConnectorTransactionHandle
{
    private final UUID uuid;

    public TablestoreTransactionHandle()
    {
        this(UUID.randomUUID());
    }

    @JsonCreator
    public TablestoreTransactionHandle(@JsonProperty("uuid") UUID uuid)
    {
        this.uuid = requireNonNull(uuid, "uuid is null");
    }

    @JsonProperty
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TablestoreTransactionHandle that = (TablestoreTransactionHandle) o;
        return Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(uuid);
    }

    @Override
    public String toString()
    {
        return "TablestoreTransactionHandle{uuid=" + uuid + "}";
    }
}
