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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.spi.security.ConnectorIdentity;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MetastoreContext
{
    private final String username;

    public MetastoreContext(ConnectorIdentity identity)
    {
        requireNonNull(identity, "identity is null");
        this.username = requireNonNull(identity.getUser(), "identity.getUser() is null");
    }

    public MetastoreContext(String username)
    {
        this.username = requireNonNull(username, "username is null");
    }

    public String getUsername()
    {
        return username;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("username", username)
                .toString();
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

        MetastoreContext other = (MetastoreContext) o;
        return Objects.equals(username, other.username);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(username);
    }
}
