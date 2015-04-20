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

public class SqlDataAccessCharacteristic
        extends RoutineCharacteristic
{
    public static final SqlDataAccessCharacteristic NO_SQL = new SqlDataAccessCharacteristic(RoutineCharacteristics.SqlDataAccessType.NO_SQL);
    public static final SqlDataAccessCharacteristic CONTAINS_SQL = new SqlDataAccessCharacteristic(RoutineCharacteristics.SqlDataAccessType.CONTAINS_SQL);
    public static final SqlDataAccessCharacteristic READS_SQL_DATA = new SqlDataAccessCharacteristic(RoutineCharacteristics.SqlDataAccessType.READS_SQL_DATA);
    public static final SqlDataAccessCharacteristic MODIFIES_SQL_DATA = new SqlDataAccessCharacteristic(RoutineCharacteristics.SqlDataAccessType.MODIFIES_SQL_DATA);

    private final RoutineCharacteristics.SqlDataAccessType type;

    private SqlDataAccessCharacteristic(RoutineCharacteristics.SqlDataAccessType type)
    {
        this.type = type;
    }

    public RoutineCharacteristics.SqlDataAccessType getType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSqlDataAccessCharacteristic(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type);
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
        SqlDataAccessCharacteristic o = (SqlDataAccessCharacteristic) obj;
        return type == o.type;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .toString();
    }
}
