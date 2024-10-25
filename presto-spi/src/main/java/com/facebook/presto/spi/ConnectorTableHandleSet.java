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
package com.facebook.presto.spi;

import com.google.common.base.Joiner;

import java.util.Objects;
import java.util.Set;

public class ConnectorTableHandleSet
        implements ConnectorTableHandle
{
    private final Set<ConnectorTableHandle> connectorTableHandles;

    public ConnectorTableHandleSet(Set<ConnectorTableHandle> connectorTableHandles)
    {
        this.connectorTableHandles = connectorTableHandles;
    }

    public Set<ConnectorTableHandle> getConnectorTableHandles()
    {
        return connectorTableHandles;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ConnectorTableHandleSet o = (ConnectorTableHandleSet) obj;
        return Objects.equals(this.connectorTableHandles, o.connectorTableHandles);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorTableHandles);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").useForNull("null").join(connectorTableHandles.iterator());
    }
}
