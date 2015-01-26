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

public class DeterministicCharacteristic
        extends RoutineCharacteristic
{
    public static final DeterministicCharacteristic DETERMINISTIC = new DeterministicCharacteristic(true);
    public static final DeterministicCharacteristic NOT_DETERMINISTIC = new DeterministicCharacteristic(false);

    private final boolean deterministic;

    private DeterministicCharacteristic(boolean deterministic)
    {
        this(Optional.empty(), deterministic);
    }

    public DeterministicCharacteristic(NodeLocation location, boolean deterministic)
    {
        this(Optional.of(location), deterministic);
    }

    public DeterministicCharacteristic(Optional<NodeLocation> location, boolean deterministic)
    {
        super(location);
        this.deterministic = deterministic;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDeterministicCharacteristic(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(deterministic);
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
        DeterministicCharacteristic o = (DeterministicCharacteristic) obj;
        return deterministic == o.deterministic;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("deterministic", deterministic)
                .toString();
    }
}
