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
package com.facebook.presto.tpcds;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class TpcdsTableHandle
        implements ConnectorTableHandle
{
    private final String tableName;
    private final double scaleFactor;

    @JsonCreator
    @ThriftConstructor
    public TpcdsTableHandle(@JsonProperty("tableName") String tableName, @JsonProperty("scaleFactor") double scaleFactor)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        checkState(scaleFactor > 0, "scaleFactor is negative");
        this.scaleFactor = scaleFactor;
    }

    @JsonProperty
    @ThriftField(1)
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    @ThriftField(2)
    public double getScaleFactor()
    {
        return scaleFactor;
    }

    @Override
    public String toString()
    {
        return "tpcds:" + tableName + ":sf" + scaleFactor;
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
        TpcdsTableHandle other = (TpcdsTableHandle) obj;
        return Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.scaleFactor, other.scaleFactor);
    }
}
