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
package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final KafkaTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public KafkaTableLayoutHandle(@JsonProperty("table") KafkaTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this.table = table;
        this.tupleDomain = tupleDomain;
    }
    @JsonProperty
    public KafkaTableHandle getTable()
    {
        return table;
    }
    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((table == null) ? 0 : table.hashCode());
        result = prime * result + ((tupleDomain == null) ? 0 : tupleDomain.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KafkaTableLayoutHandle other = (KafkaTableLayoutHandle) obj;
        if (table == null) {
            if (other.table != null) {
                return false;
            }
        }
        else if (!table.equals(other.table)) {
            return false;
        }
        if (tupleDomain == null) {
            if (other.tupleDomain != null) {
                return false;
            }
        }
        else if (!tupleDomain.equals(other.tupleDomain)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "KafkaTableLayoutHandle [table=" + table + ", tupleDomain=" + tupleDomain + "]";
    }
}
