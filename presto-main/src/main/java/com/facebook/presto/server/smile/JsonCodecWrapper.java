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
package com.facebook.presto.server.smile;

import com.facebook.airlift.json.JsonCodec;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class JsonCodecWrapper<T>
        implements Codec<T>
{
    private final JsonCodec<T> jsonCodec;

    private JsonCodecWrapper(JsonCodec<T> jsonCodec)
    {
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
    }

    public static <T> JsonCodecWrapper<T> wrapJsonCodec(JsonCodec<T> codec)
    {
        return new JsonCodecWrapper<>(codec);
    }

    public static <T> JsonCodec<T> unwrapJsonCodec(Codec<T> codec)
    {
        verify(codec instanceof JsonCodecWrapper);
        return ((JsonCodecWrapper<T>) codec).jsonCodec;
    }

    @Override
    public byte[] toBytes(T instance)
    {
        return jsonCodec.toJsonBytes(instance);
    }

    @Override
    public T fromBytes(byte[] bytes)
    {
        return jsonCodec.fromJson(bytes);
    }
}
