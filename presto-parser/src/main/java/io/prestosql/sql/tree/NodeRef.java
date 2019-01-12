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
package io.prestosql.sql.tree;

import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public final class NodeRef<T extends Node>
{
    public static <T extends Node> NodeRef<T> of(T node)
    {
        return new NodeRef<>(node);
    }

    private final T node;

    private NodeRef(T node)
    {
        this.node = requireNonNull(node, "node is null");
    }

    public T getNode()
    {
        return node;
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
        NodeRef<?> other = (NodeRef<?>) o;
        return node == other.node;
    }

    @Override
    public int hashCode()
    {
        return identityHashCode(node);
    }

    @Override
    public String toString()
    {
        return format(
                "@%s: %s",
                Integer.toHexString(identityHashCode(node)),
                node);
    }
}
