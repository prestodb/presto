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

import static com.google.common.base.MoreObjects.toStringHelper;

public class ReturnedResultSetsCharacteristic
        extends RoutineCharacteristic
{
    private final int returnedResultSets;

    public ReturnedResultSetsCharacteristic(int returnedResultSets)
    {
        this.returnedResultSets = returnedResultSets;
    }

    public int getReturnedResultSets()
    {
        return returnedResultSets;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitReturnedResultSetsCharacteristic(this, context);
    }

    @Override
    public int hashCode()
    {
        return returnedResultSets;
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
        ReturnedResultSetsCharacteristic o = (ReturnedResultSetsCharacteristic) obj;
        return returnedResultSets == o.returnedResultSets;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("returnedResultSets", returnedResultSets)
                .toString();
    }
}
