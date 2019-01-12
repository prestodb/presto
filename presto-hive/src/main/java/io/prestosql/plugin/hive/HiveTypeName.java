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
package io.prestosql.plugin.hive;

import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class HiveTypeName
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HivePartitionKey.class).instanceSize() +
            ClassLayout.parseClass(String.class).instanceSize();

    private final String value;

    public HiveTypeName(String value)
    {
        this.value = requireNonNull(value, "value is null");
    }

    @Override
    public String toString()
    {
        return value;
    }

    public HiveType toHiveType()
    {
        return HiveType.valueOf(value);
    }

    public int getEstimatedSizeInBytes()
    {
        return INSTANCE_SIZE + value.length() * Character.BYTES;
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
        HiveTypeName that = (HiveTypeName) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }
}
