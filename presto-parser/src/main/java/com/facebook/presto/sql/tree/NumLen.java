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

import static com.google.common.base.Preconditions.checkNotNull;

public final class NumLen
        extends Node
{
    private final String precision;
    private final Optional<String> scale;

    public NumLen(String precision, Optional<String> scale)
    {
        this.precision = checkNotNull(precision, "precision is null");
        this.scale = scale;
    }

    public String getPrecision()
    {
        return precision;
    }

    public Optional<String> getScale()
    {
        return scale;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNumLen(this, context);
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
        NumLen o = (NumLen) obj;
        return Objects.equal(precision, o.precision)
                && Objects.equal(scale, o.scale);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(precision, scale);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("precision", precision)
                .add("scale", scale)
                .toString();
    }
}
