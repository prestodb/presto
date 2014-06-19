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

import com.google.common.base.Objects;
import com.google.common.base.Optional;

public final class NumType
        extends DataType
{
    private final Optional<NumLen> numLength;

    public NumType(TypeName type, Optional<NumLen> length)
    {
        super(type);
        this.numLength = length;
    }

    public Optional<NumLen> getNumLength()
    {
        return numLength;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNumType(this, context);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        NumType o = (NumType) obj;
        return Objects.equal(type, o.type)
                && Objects.equal(numLength, o.numLength);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type, numLength);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("type", type)
                .add("numLength", numLength)
                .toString();
    }
}
