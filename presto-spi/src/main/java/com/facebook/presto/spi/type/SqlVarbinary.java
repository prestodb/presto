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
package com.facebook.presto.spi.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

public final class SqlVarbinary
{
    private final byte[] bytes;

    public SqlVarbinary(byte[] bytes)
    {
        this.bytes = requireNonNull(bytes, "bytes is null");
    }

    @JsonValue
    public byte[] getBytes()
    {
        return bytes;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(bytes);
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
        SqlVarbinary other = (SqlVarbinary) obj;
        return Arrays.equals(bytes, other.bytes);
    }
}
