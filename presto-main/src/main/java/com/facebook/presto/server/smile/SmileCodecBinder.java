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

import com.google.common.annotations.Beta;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.internal.MoreTypes.ParameterizedTypeImpl;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Beta
public class SmileCodecBinder
{
    private final Binder binder;

    public static SmileCodecBinder smileCodecBinder(Binder binder)
    {
        return new SmileCodecBinder(binder);
    }

    private SmileCodecBinder(Binder binder)
    {
        this.binder = requireNonNull(binder, "binder is null").skipSources(getClass());
    }

    public void bindSmileCodec(Class<?> type)
    {
        requireNonNull(type, "type is null");

        binder.bind(getSmileCodecKey(type)).toProvider(new SmileCodecProvider(type)).in(Scopes.SINGLETON);
    }

    public void bindSmileCodec(TypeLiteral<?> type)
    {
        requireNonNull(type, "type is null");

        binder.bind(getSmileCodecKey(type.getType())).toProvider(new SmileCodecProvider(type.getType())).in(Scopes.SINGLETON);
    }

    public void bindListSmileCodec(Class<?> type)
    {
        requireNonNull(type, "type is null");

        ParameterizedTypeImpl listType = new ParameterizedTypeImpl(null, List.class, type);
        binder.bind(getSmileCodecKey(listType)).toProvider(new SmileCodecProvider(listType)).in(Scopes.SINGLETON);
    }

    public void bindListSmileCodec(SmileCodec<?> type)
    {
        requireNonNull(type, "type is null");

        ParameterizedTypeImpl listType = new ParameterizedTypeImpl(null, List.class, type.getType());
        binder.bind(getSmileCodecKey(listType)).toProvider(new SmileCodecProvider(listType)).in(Scopes.SINGLETON);
    }

    public void bindMapSmileCodec(Class<?> keyType, Class<?> valueType)
    {
        requireNonNull(keyType, "keyType is null");
        requireNonNull(valueType, "valueType is null");

        ParameterizedTypeImpl mapType = new ParameterizedTypeImpl(null, Map.class, keyType, valueType);
        binder.bind(getSmileCodecKey(mapType)).toProvider(new SmileCodecProvider(mapType)).in(Scopes.SINGLETON);
    }

    public void bindMapSmileCodec(Class<?> keyType, SmileCodec<?> valueType)
    {
        requireNonNull(keyType, "keyType is null");
        requireNonNull(valueType, "valueType is null");

        ParameterizedTypeImpl mapType = new ParameterizedTypeImpl(null, Map.class, keyType, valueType.getType());
        binder.bind(getSmileCodecKey(mapType)).toProvider(new SmileCodecProvider(mapType)).in(Scopes.SINGLETON);
    }

    @SuppressWarnings("unchecked")
    private Key<SmileCodec<?>> getSmileCodecKey(Type type)
    {
        return (Key<SmileCodec<?>>) Key.get(new ParameterizedTypeImpl(null, SmileCodec.class, type));
    }
}
