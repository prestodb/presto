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
package com.facebook.presto.functionNamespace;

import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class UuidFunctionNamespaceTransactionHandle
        implements FunctionNamespaceTransactionHandle
{
    private final UUID uuid;

    private UuidFunctionNamespaceTransactionHandle(UUID uuid)
    {
        this.uuid = requireNonNull(uuid, "uuid is null");
    }

    public static UuidFunctionNamespaceTransactionHandle create()
    {
        return new UuidFunctionNamespaceTransactionHandle(UUID.randomUUID());
    }

    @JsonCreator
    public static UuidFunctionNamespaceTransactionHandle valueOf(String value)
    {
        return new UuidFunctionNamespaceTransactionHandle(UUID.fromString(value));
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
        final UuidFunctionNamespaceTransactionHandle o = (UuidFunctionNamespaceTransactionHandle) obj;
        return Objects.equals(uuid, o.uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(uuid);
    }

    @Override
    @JsonValue
    public String toString()
    {
        return uuid.toString();
    }
}
