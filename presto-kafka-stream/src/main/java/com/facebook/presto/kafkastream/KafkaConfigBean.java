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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class KafkaConfigBean
{
    private Map<String, KafkaTable> kafkaTableMap;

    @JsonCreator
    public KafkaConfigBean(@JsonProperty("tables") List<KafkaTable> kafkaTables)
    {
        requireNonNull(kafkaTables);
        kafkaTableMap = new HashMap<>();
        kafkaTables.stream().forEach((table) -> kafkaTableMap.put(table.getName(), table));
    }

    public KafkaTable getKafkaTable(String tableName)
    {
        return kafkaTableMap.get(tableName);
    }

    public Map<String, KafkaTable> getKafkaTableMap()
    {
        return kafkaTableMap;
    }

    public void setKafkaTableMap(Map<String, KafkaTable> kafkaTableMap)
    {
        this.kafkaTableMap = kafkaTableMap;
    }

    @Override
    public String toString()
    {
        return "KafkaConfigBean [kafkaTableMap=" + kafkaTableMap + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((kafkaTableMap == null) ? 0 : kafkaTableMap.hashCode());
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
        KafkaConfigBean other = (KafkaConfigBean) obj;
        if (kafkaTableMap == null) {
            if (other.kafkaTableMap != null) {
                return false;
            }
        }
        else if (!kafkaTableMap.equals(other.kafkaTableMap)) {
            return false;
        }
        return true;
    }
}
