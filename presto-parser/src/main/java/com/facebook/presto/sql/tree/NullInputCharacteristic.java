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

import static com.google.common.base.MoreObjects.toStringHelper;

public class NullInputCharacteristic
        extends RoutineCharacteristic
{
    public static final NullInputCharacteristic RETURNS_NULL_ON_NULL_INPUT = new NullInputCharacteristic(true);
    public static final NullInputCharacteristic CALLED_ON_NULL_INPUT = new NullInputCharacteristic(false);

    private final boolean returningNull;

    private NullInputCharacteristic(boolean returningNull)
    {
        this.returningNull = returningNull;
    }

    public boolean isReturningNull()
    {
        return returningNull;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNullInputCharacteristic(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(returningNull);
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
        NullInputCharacteristic o = (NullInputCharacteristic) obj;
        return returningNull == o.returningNull;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("returningNull", returningNull)
                .toString();
    }
}
