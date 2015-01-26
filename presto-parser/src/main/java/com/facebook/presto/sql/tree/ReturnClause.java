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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class ReturnClause
        extends Node
{
    private final String returnType;
    private final Optional<String> castFromType;

    public ReturnClause(String returnType, Optional<String> castFromType)
    {
        this.returnType = checkNotNull(returnType, "returnType is null");
        this.castFromType = checkNotNull(castFromType, "castFromType is null");
    }

    public String getReturnType()
    {
        return returnType;
    }

    public Optional<String> getCastFromType()
    {
        return castFromType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitReturnClause(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(returnType, castFromType);
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
        ReturnClause o = (ReturnClause) obj;
        return Objects.equals(returnType, o.returnType) &&
                Objects.equals(castFromType, o.castFromType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("returnType", returnType)
                .add("castFromType", castFromType)
                .toString();
    }
}
