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
package com.facebook.presto.spi;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.Optional;

@Immutable
public class TableSample
{
    private final SchemaTableName tableName;
    private final Optional<Integer> samplingPercentage;
    private final Optional<String> samplingColumnName;

    public TableSample(SchemaTableName tableName)
    {
        this(tableName, Optional.empty());
    }

    public TableSample(SchemaTableName tableName, Optional<Integer> samplingPercentage)
    {
        this(tableName, samplingPercentage, Optional.empty());
    }

    public TableSample(SchemaTableName tableName, Optional<Integer> samplingPercentage, Optional<String> samplingColumnName)
    {
        this.tableName = tableName;
        this.samplingPercentage = samplingPercentage;
        this.samplingColumnName = samplingColumnName;
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public Optional<Integer> getSamplingPercentage()
    {
        return samplingPercentage;
    }

    public Optional<String> getSamplingColumnName()
    {
        return samplingColumnName;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("SampleTable{");
        sb.append("name='").append(tableName);
        sb.append(", pct=").append(samplingPercentage.isPresent() ? (int) samplingPercentage.get() : 0);
        sb.append(", column=").append(samplingColumnName.isPresent() ? samplingColumnName.get() : "");
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.toString());
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
        TableSample other = (TableSample) obj;
        return Objects.equals(this.toString(), other.toString());
    }
}
