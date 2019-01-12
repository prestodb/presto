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
package io.prestosql.transaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public final class TransactionId
{
    private final UUID uuid;

    private TransactionId(UUID uuid)
    {
        this.uuid = requireNonNull(uuid, "uuid is null");
    }

    public static TransactionId create()
    {
        return new TransactionId(UUID.randomUUID());
    }

    @JsonCreator
    public static TransactionId valueOf(String value)
    {
        return new TransactionId(UUID.fromString(value));
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
    public String toString()
    {
        return uuid.toString();
    }
}
