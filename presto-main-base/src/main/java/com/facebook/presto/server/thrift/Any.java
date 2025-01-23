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
package com.facebook.presto.server.thrift;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;

import static java.util.Objects.requireNonNull;

/**
 * Any is used for the serde of Connector specific types.
 * <p>
 * Any wraps the type information in an id field along
 * with the serialized form of the object in a byte array.
 */
@ThriftStruct
public class Any
{
    private final String id;
    private final byte[] bytes;

    @ThriftConstructor
    public Any(String id, byte[] bytes)
    {
        this.id = requireNonNull(id, "id is null");
        this.bytes = requireNonNull(bytes, "bytes is null");
    }

    @ThriftField(1)
    public String getId()
    {
        return id;
    }

    @ThriftField(2)
    public byte[] getBytes()
    {
        return bytes;
    }
}
