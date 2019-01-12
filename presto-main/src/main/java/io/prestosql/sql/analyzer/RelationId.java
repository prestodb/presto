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
package io.prestosql.sql.analyzer;

import io.prestosql.sql.tree.Node;

import javax.annotation.Nullable;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public class RelationId
{
    /**
     * Creates {@link RelationId} equal to any {@link RelationId} created from exactly the same source.
     */
    public static RelationId of(Node sourceNode)
    {
        return new RelationId(requireNonNull(sourceNode, "source cannot be null"));
    }

    /**
     * Creates {@link RelationId} equal only to itself
     */
    public static RelationId anonymous()
    {
        return new RelationId(null);
    }

    @Nullable
    private final Node sourceNode;

    private RelationId(Node sourceNode)
    {
        this.sourceNode = sourceNode;
    }

    public boolean isAnonymous()
    {
        return sourceNode == null;
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
        RelationId that = (RelationId) o;
        return sourceNode != null && sourceNode == that.sourceNode;
    }

    @Override
    public int hashCode()
    {
        return identityHashCode(sourceNode);
    }

    @Override
    public String toString()
    {
        if (isAnonymous()) {
            return toStringHelper(this)
                    .addValue("anonymous")
                    .addValue(format("x%08x", identityHashCode(this)))
                    .toString();
        }
        else {
            return toStringHelper(this)
                    .addValue(sourceNode.getClass().getSimpleName())
                    .addValue(format("x%08x", identityHashCode(sourceNode)))
                    .toString();
        }
    }
}
