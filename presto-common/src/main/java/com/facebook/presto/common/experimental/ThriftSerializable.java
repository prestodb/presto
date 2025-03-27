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
package com.facebook.presto.common.experimental;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public interface ThriftSerializable<T extends TBase<?, ?>>
{
    default String getImplementationType()
    {
        return getClass().getSimpleName();
    }

    default T toThriftInterface()
    {
        return toThrift();
    }

    default T toThrift()
    {
        return null;
    }

    default byte[] serialize()
    {
        try {
            TSerializer serializer = new TSerializer();
            return serializer.serialize(toThriftInterface());
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
