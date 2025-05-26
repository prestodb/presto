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
package com.facebook.presto.common.transaction;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static java.util.UUID.fromString;

@ThriftStruct
public final class TransactionId
{
    private final UUID uuid;

    public TransactionId(UUID uuid)
    {
        this.uuid = requireNonNull(uuid, "uuid is null");
    }

    @ThriftConstructor
    public TransactionId(String uuid)
    {
        this(fromString(uuid));
    }

    public static TransactionId create()
    {
        return new TransactionId(UUID.randomUUID());
    }

    @JsonCreator
    public static TransactionId valueOf(String value)
    {
        return new TransactionId(fromString(value));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(uuid);
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
        final TransactionId other = (TransactionId) obj;
        return Objects.equals(this.uuid, other.uuid);
    }

    @Override
    @JsonValue
    @ThriftField(value = 1, name = "uuid")
    public String toString()
    {
        return uuid.toString();
    }

    public UUID getUuid()
    {
        return uuid;
    }
}
