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
package com.facebook.presto.hdfs;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSConnectorId
{
    private final String connectorId;

    public HDFSConnectorId(String connectorId)
    {
        requireNonNull(connectorId, "connectorId is null");
        this.connectorId = connectorId;
    }

    public String getId()
    {
        return connectorId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId);
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
        HDFSConnectorId other = (HDFSConnectorId) obj;
        return Objects.equals(this.connectorId, other.connectorId);
    }

    @Override
    public String toString()
    {
        return connectorId;
    }
}
