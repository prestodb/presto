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

public class TableElement
        extends Statement
{
    private final String name;
    private final DataType dataType;
    private final Optional<ColumnConstDef> columnConstDef;

    public TableElement(
            String name,
            DataType dataType,
            Optional<ColumnConstDef> columnConstDef)
    {
        checkNotNull(name, "name is null");
        checkNotNull(dataType, "dataType is null");

        this.name = name;
        this.dataType = dataType;
        this.columnConstDef = columnConstDef;
    }

    public String getName()
    {
        return name;
    }

    public DataType getDataType()
    {
        return dataType;
    }

    public Optional<ColumnConstDef> getColumnConstDef()
    {
        return columnConstDef;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableElement(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("dataType", dataType)
                .add("columnConstDef", columnConstDef.orNull())
                .omitNullValues()
                .toString();
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
        TableElement o = (TableElement) obj;
        return Objects.equal(name, o.name) &&
                Objects.equal(dataType, o.dataType) &&
                Objects.equal(columnConstDef, o.columnConstDef);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, dataType, columnConstDef);
    }
}
