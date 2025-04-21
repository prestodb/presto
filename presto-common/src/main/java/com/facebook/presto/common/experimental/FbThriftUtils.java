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

import com.facebook.thrift.payload.Reader;
import com.facebook.thrift.util.SerializationProtocol;
import com.facebook.thrift.util.SerializerUtil;

import java.lang.reflect.InvocationTargetException;

public class FbThriftUtils
{
    private FbThriftUtils() {}

    public static byte[] serialize(com.facebook.thrift.payload.ThriftSerializable thriftStruct)
    {
        return SerializerUtil.toByteArray(thriftStruct, SerializationProtocol.TSimpleJSONBase64);
    }

    public static <T extends com.facebook.thrift.payload.ThriftSerializable> T deserialize(Class<T> clazz, byte[] src)
    {
        try {
            Reader<T> reader = (Reader<T>) clazz.getMethod("asReader").invoke(null);
            return SerializerUtil.fromByteArray(reader, src, SerializationProtocol.TSimpleJSONBase64);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
