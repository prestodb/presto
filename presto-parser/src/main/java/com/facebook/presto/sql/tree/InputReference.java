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

import java.util.Optional;

/**
 * A reference to an execution engine channel.
 * <p>
 * This is used to replace a {@link QualifiedNameReference} with a direct reference to the physical
 * channel and field to avoid unnecessary lookups in a symbol->channel map during evaluation
 */
public class InputReference
        extends Expression
{
    private final int channel;

    public InputReference(int channel)
    {
        this(Optional.empty(), channel);
    }

    public InputReference(NodeLocation location, int channel)
    {
        this(Optional.of(location), channel);
    }

    private InputReference(Optional<NodeLocation> location, int channel)
    {
        super(location);
        this.channel = channel;
    }

    public int getChannel()
    {
        return channel;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitInputReference(this, context);
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

        InputReference that = (InputReference) o;

        return channel == that.channel;
    }

    @Override
    public int hashCode()
    {
        return channel;
    }
}
