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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class RoutineCharacteristics
        extends Node
{
    public static enum SqlDataAccessType {
        NO_SQL,
        CONTAINS_SQL,
        READS_SQL_DATA,
        MODIFIES_SQL_DATA
    }

    private final List<QualifiedName> specificCharacteristics;
    private final boolean isDeterministic;
    private final SqlDataAccessType sqlDataAccessType;
    private final boolean returnsNullOnNullInput;
    private final int dynamicResultSets;

    public RoutineCharacteristics(List<RoutineCharacteristic> routineCharacteristics)
    {
        List<QualifiedName> specificCharacteristics = new ArrayList<>();
        boolean isDeterministic = false;
        SqlDataAccessType sqlDataAccessType = SqlDataAccessType.CONTAINS_SQL;
        boolean returnsNullOnNullInput = true;
        int dynamicResultSets = 0;

        for (RoutineCharacteristic routineCharacteristic : routineCharacteristics) {
            if (routineCharacteristic instanceof SpecificCharacteristic) {
                specificCharacteristics.add(((SpecificCharacteristic) routineCharacteristic).getValue());
            }
            else if (routineCharacteristic instanceof DeterministicCharacteristic) {
                isDeterministic = ((DeterministicCharacteristic) routineCharacteristic).isDeterministic();
            }
            else if (routineCharacteristic instanceof SqlDataAccessCharacteristic) {
                sqlDataAccessType = ((SqlDataAccessCharacteristic) routineCharacteristic).getType();
            }
            else if (routineCharacteristic instanceof NullInputCharacteristic) {
                returnsNullOnNullInput = ((NullInputCharacteristic) routineCharacteristic).isReturningNull();
            }
            else if (routineCharacteristic instanceof ReturnedResultSetsCharacteristic) {
                dynamicResultSets = ((ReturnedResultSetsCharacteristic) routineCharacteristic).getReturnedResultSets();
            }
        }

        this.specificCharacteristics = ImmutableList.copyOf(specificCharacteristics);
        this.isDeterministic = isDeterministic;
        this.returnsNullOnNullInput = returnsNullOnNullInput;
        this.sqlDataAccessType = sqlDataAccessType;
        this.dynamicResultSets = dynamicResultSets;
    }

    public int getDynamicResultSets()
    {
        return dynamicResultSets;
    }

    public boolean isDeterministic()
    {
        return isDeterministic;
    }

    public boolean isReturnsNullOnNullInput()
    {
        return returnsNullOnNullInput;
    }

    public List<QualifiedName> getSpecificCharacteristics()
    {
        return specificCharacteristics;
    }

    public SqlDataAccessType getSqlDataAccessType()
    {
        return sqlDataAccessType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRoutineCharacteristics(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                specificCharacteristics,
                isDeterministic,
                sqlDataAccessType,
                returnsNullOnNullInput,
                dynamicResultSets);
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
        RoutineCharacteristics o = (RoutineCharacteristics) obj;
        return Objects.equals(specificCharacteristics, o.specificCharacteristics) &&
                isDeterministic == o.isDeterministic &&
                sqlDataAccessType == o.sqlDataAccessType &&
                returnsNullOnNullInput == o.returnsNullOnNullInput &&
                dynamicResultSets == o.dynamicResultSets;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("specificCharacteristics", specificCharacteristics)
                .add("isDeterministic", isDeterministic)
                .add("sqlDataAccessType", sqlDataAccessType)
                .add("returnsNullOnNullInput", returnsNullOnNullInput)
                .add("dynamicResultSets", dynamicResultSets)
                .toString();
    }
}
