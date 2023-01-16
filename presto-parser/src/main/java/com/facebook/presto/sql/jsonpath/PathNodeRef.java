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
package com.facebook.presto.sql.jsonpath;

import com.facebook.presto.sql.jsonpath.tree.PathNode;

import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public final class PathNodeRef<T extends PathNode>
{
    public static <T extends PathNode> PathNodeRef<T> of(T pathNode)
    {
        return new PathNodeRef<>(pathNode);
    }

    private final T pathNode;

    private PathNodeRef(T pathNode)
    {
        this.pathNode = requireNonNull(pathNode, "pathNode is null");
    }

    public T getNode()
    {
        return pathNode;
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
        PathNodeRef<?> other = (PathNodeRef<?>) o;
        return pathNode == other.pathNode;
    }

    @Override
    public int hashCode()
    {
        return identityHashCode(pathNode);
    }

    @Override
    public String toString()
    {
        return format(
                "@%s: %s",
                Integer.toHexString(identityHashCode(pathNode)),
                pathNode);
    }
}
