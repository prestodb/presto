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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class IrDisjunctionPredicate
        extends IrPredicate
{
    private final IrPredicate left;
    private final IrPredicate right;

    @JsonCreator
    public IrDisjunctionPredicate(@JsonProperty("left") IrPredicate left, @JsonProperty("right") IrPredicate right)
    {
        super();
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
    }

    @Override
    protected <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrDisjunctionPredicate(this, context);
    }

    @JsonProperty
    public IrPathNode getLeft()
    {
        return left;
    }

    @JsonProperty
    public IrPathNode getRight()
    {
        return right;
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
        IrDisjunctionPredicate other = (IrDisjunctionPredicate) obj;
        return Objects.equals(this.left, other.left) &&
                Objects.equals(this.right, other.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(left, right);
    }
}
