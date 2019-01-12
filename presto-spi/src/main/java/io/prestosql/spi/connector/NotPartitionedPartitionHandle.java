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
package io.prestosql.spi.connector;

public final class NotPartitionedPartitionHandle
        extends ConnectorPartitionHandle
{
    public static final ConnectorPartitionHandle NOT_PARTITIONED = new NotPartitionedPartitionHandle();

    private NotPartitionedPartitionHandle() {}

    @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass", "ObjectEquality"})
    @Override
    public boolean equals(Object obj)
    {
        return obj == NOT_PARTITIONED;
    }

    @Override
    public int hashCode()
    {
        return 0;
    }
}
