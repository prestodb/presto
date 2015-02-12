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
package com.facebook.presto.metadata;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class NodeVersion
{
    public static final NodeVersion UNKNOWN = new NodeVersion("<unknown>");

    private final String version;

    public NodeVersion(String version)
    {
        this.version = checkNotNull(version, "version is null");
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

        NodeVersion that = (NodeVersion) o;
        return Objects.equals(version, that.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version);
    }

    @Override
    public String toString()
    {
        return version;
    }
}
