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

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IrMemberAccessor
        extends IrAccessor
{
    // object member key or Optional.empty for wildcard member accessor
    private final Optional<String> key;

    @JsonCreator
    public IrMemberAccessor(@JsonProperty("base") IrPathNode base, @JsonProperty("key") Optional<String> key, @JsonProperty("type") Optional<Type> type)
    {
        super(base, type);
        this.key = requireNonNull(key, "key is null");
    }

    @Override
    protected <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrMemberAccessor(this, context);
    }

    @JsonProperty
    public Optional<String> getKey()
    {
        return key;
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
        IrMemberAccessor other = (IrMemberAccessor) obj;
        return Objects.equals(this.base, other.base) && Objects.equals(this.key, other.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base, key);
    }
}
