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
package com.facebook.presto.plugin.jdbc;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

final class RemoteTableNameCacheKey
{
    private final JdbcIdentity identity;
    private final String schema;

    RemoteTableNameCacheKey(JdbcIdentity identity, String schema)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    JdbcIdentity getIdentity()
    {
        return identity;
    }

    String getSchema()
    {
        return schema;
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
        RemoteTableNameCacheKey that = (RemoteTableNameCacheKey) o;
        return Objects.equals(identity, that.identity) &&
                Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identity, schema);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("identity", identity)
                .add("schema", schema)
                .toString();
    }
}
