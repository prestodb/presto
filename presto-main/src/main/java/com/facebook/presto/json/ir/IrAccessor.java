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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class IrAccessor
        extends IrPathNode
{
    protected final IrPathNode base;

    IrAccessor(IrPathNode base, Optional<Type> type)
    {
        super(type);
        this.base = requireNonNull(base, "accessor base is null");
    }

    @Override
    protected <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrAccessor(this, context);
    }

    @JsonProperty
    public IrPathNode getBase()
    {
        return base;
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
        IrAccessor other = (IrAccessor) obj;
        return Objects.equals(this.base, other.base);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base);
    }
}
