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

public final class CharType
        extends DataType
{
    private final Optional<String> strLength;

    public CharType(TypeName type, Optional<String> length)
    {
        super(type);
        this.strLength = length;
    }

    public Optional<String> getStrLength()
    {
        return strLength;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCharType(this, context);
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
        CharType o = (CharType) obj;
        return Objects.equal(type, o.type)
                && Objects.equal(strLength, o.strLength);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type, strLength);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("type", type)
                .add("strLength", strLength)
                .toString();
    }
}
