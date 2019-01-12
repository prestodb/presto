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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class TransactionAccessMode
        extends TransactionMode
{
    private final boolean readOnly;

    public TransactionAccessMode(boolean readOnly)
    {
        this(Optional.empty(), readOnly);
    }

    public TransactionAccessMode(NodeLocation location, boolean readOnly)
    {
        this(Optional.of(location), readOnly);
    }

    private TransactionAccessMode(Optional<NodeLocation> location, boolean readOnly)
    {
        super(location);
        this.readOnly = readOnly;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTransactionAccessMode(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(readOnly);
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
        final TransactionAccessMode other = (TransactionAccessMode) obj;
        return this.readOnly == other.readOnly;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("readOnly", readOnly)
                .toString();
    }
}
