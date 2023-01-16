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
package com.facebook.presto.json.ir;

import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IrArrayAccessor
        extends IrAccessor
{
    // list of subscripts or empty list for wildcard array accessor
    private final List<Subscript> subscripts;

    @JsonCreator
    public IrArrayAccessor(@JsonProperty("base") IrPathNode base, @JsonProperty("subscripts") List<Subscript> subscripts, @JsonProperty("type") Optional<Type> type)
    {
        super(base, type);
        this.subscripts = requireNonNull(subscripts, "subscripts is null");
    }

    @Override
    protected <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrArrayAccessor(this, context);
    }

    @JsonProperty
    public List<Subscript> getSubscripts()
    {
        return subscripts;
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
        IrArrayAccessor other = (IrArrayAccessor) obj;
        return Objects.equals(this.base, other.base) && Objects.equals(this.subscripts, other.subscripts);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base, subscripts);
    }

    public static class Subscript
    {
        private final IrPathNode from;
        private final Optional<IrPathNode> to;

        @JsonCreator
        public Subscript(@JsonProperty("from") IrPathNode from, @JsonProperty("to") Optional<IrPathNode> to)
        {
            this.from = requireNonNull(from, "from is null");
            this.to = requireNonNull(to, "to is null");
        }

        @JsonProperty
        public IrPathNode getFrom()
        {
            return from;
        }

        @JsonProperty
        public Optional<IrPathNode> getTo()
        {
            return to;
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
            Subscript other = (Subscript) obj;
            return Objects.equals(this.from, other.from) && Objects.equals(this.to, other.to);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(from, to);
        }
    }
}
