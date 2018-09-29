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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

public class KafkaColumnMetadata
        extends ColumnMetadata
{
    private final String jsonPath;

    public KafkaColumnMetadata(String name, Type type, String jsonPath)
    {
        super(name, type);
        this.jsonPath = jsonPath;
    }

    public KafkaColumnMetadata(KafkaColumn column)
    {
        this(column.getName(), column.getType(), column.getJsonPath());
    }

    public String getJsonPath()
    {
        return jsonPath;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((jsonPath == null) ? 0 : jsonPath.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KafkaColumnMetadata other = (KafkaColumnMetadata) obj;
        if (jsonPath == null) {
            if (other.jsonPath != null) {
                return false;
            }
        }
        else if (!jsonPath.equals(other.jsonPath)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "KafkaColumnMetadata [jsonPath=" + jsonPath + "]";
    }
}
