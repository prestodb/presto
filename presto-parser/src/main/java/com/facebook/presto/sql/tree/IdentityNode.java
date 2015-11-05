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
package com.facebook.presto.sql.tree;

import com.facebook.presto.spi.security.Identity;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class IdentityNode
        extends Node
{
    private final Identity identity;

    public IdentityNode(Identity identity)
    {
        this(Optional.empty(), identity);
    }

    public IdentityNode(NodeLocation location, Identity identity)
    {
        this(Optional.of(location), identity);
    }

    private IdentityNode(Optional<NodeLocation> location, Identity identity)
    {
        super(location);
        this.identity = requireNonNull(identity, "user/role is null");
    }

    public Identity getIdentity()
    {
        return identity;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIdentityNode(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identity);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || (getClass() != obj.getClass())) {
            return false;
        }
        IdentityNode o = (IdentityNode) obj;
        return Objects.equals(identity, o.identity);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("identity", identity)
                .toString();
    }
}
