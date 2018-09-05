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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.spi.connector.ConnectorPartitionHandle;

import java.util.Objects;

public class ThriftPartitionHandle
        extends ConnectorPartitionHandle
{
    private final int bucket;

    public ThriftPartitionHandle(int bucket)
    {
        this.bucket = bucket;
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
        ThriftPartitionHandle other = (ThriftPartitionHandle) obj;
        return this.bucket == other.bucket;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucket);
    }
}
