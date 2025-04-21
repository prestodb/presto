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

import com.facebook.presto.common.experimental.auto_gen.ThriftObject;

public class ObjectAdapter
{
    private ObjectAdapter() {}

    public static byte[] serialize(Object obj)
    {
        byte[] bytes = ThriftSerializationRegistry.serialize(obj);

        return FbThriftUtils.serialize(ThriftObject.builder()
                .setType(obj.getClass().getName())
                .setSerializedObject(bytes).build());
    }

    public static Object deserialize(byte[] bytes)
    {
        return fromThrift(FbThriftUtils.deserialize(ThriftObject.class, bytes));
    }

    public static Object fromThrift(ThriftObject thriftObj)
    {
        return ThriftSerializationRegistry.deserialize(
                thriftObj.getType(),
                thriftObj.getSerializedObject());
    }
}
