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

import static com.google.common.base.Preconditions.checkNotNull;

public class DataType
        extends Statement
{
    public enum TypeName
    {
        BOOLEAN,
        CHAR,
        VARCHAR,
        NUMERIC,
        INTEGER,
        DATE
    }

    protected final TypeName type;

    public DataType(TypeName type)
    {
        this.type = checkNotNull(type, "type is null");
    }

    public TypeName getTypeName()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDataType(this, context);
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
        DataType o = (DataType) obj;
        return Objects.equal(type, o.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("type", type)
                .toString();
    }
}
