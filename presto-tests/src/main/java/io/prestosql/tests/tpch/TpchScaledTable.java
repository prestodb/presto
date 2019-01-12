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
package io.prestosql.tests.tpch;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class TpchScaledTable
{
    private final String tableName;
    private final double scaleFactor;

    public TpchScaledTable(String tableName, double scaleFactor)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.scaleFactor = scaleFactor;
    }

    public String getTableName()
    {
        return tableName;
    }

    public double getScaleFactor()
    {
        return scaleFactor;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, scaleFactor);
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
        final TpchScaledTable other = (TpchScaledTable) obj;
        return Objects.equals(this.tableName, other.tableName) && Objects.equals(this.scaleFactor, other.scaleFactor);
    }
}
