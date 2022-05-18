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
package com.facebook.presto.hive;

import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class HivePartitionHandle
        extends ConnectorPartitionHandle
{
    private final int bucket;

    @JsonCreator
    public HivePartitionHandle(@JsonProperty("bucket") int bucket)
    {
        this.bucket = bucket;
    }

    @JsonProperty
    public int getBucket()
    {
        return bucket;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HivePartitionHandle that = (HivePartitionHandle) o;
        return bucket == that.bucket;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucket);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .toString();
    }
}
