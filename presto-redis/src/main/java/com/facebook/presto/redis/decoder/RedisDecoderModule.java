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
package com.facebook.presto.redis.decoder;

import com.facebook.presto.redis.decoder.csv.CsvRedisDecoderModule;
import com.facebook.presto.redis.decoder.dummy.DummyRedisDecoderModule;
import com.facebook.presto.redis.decoder.json.JsonRedisDecoderModule;
import com.facebook.presto.redis.decoder.raw.RawRedisDecoderModule;
import com.facebook.presto.redis.decoder.hash.HashRedisDecoderModule;
import com.facebook.presto.redis.decoder.zset.ZsetRedisDecoderModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

/**
 * Redis decoder specific module. Installs the registry and all known decoder submodules.
 */
public class RedisDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(RedisDecoderRegistry.class).in(Scopes.SINGLETON);

        binder.install(new DummyRedisDecoderModule());
        binder.install(new CsvRedisDecoderModule());
        binder.install(new JsonRedisDecoderModule());
        binder.install(new RawRedisDecoderModule());
        binder.install(new HashRedisDecoderModule());
        binder.install(new ZsetRedisDecoderModule());
    }

    public static void bindRowDecoder(Binder binder, Class<? extends RedisRowDecoder> decoderClass)
    {
        Multibinder<RedisRowDecoder> rowDecoderBinder = Multibinder.newSetBinder(binder, RedisRowDecoder.class);
        rowDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }

    public static void bindFieldDecoder(Binder binder, Class<? extends RedisFieldDecoder<?>> decoderClass)
    {
        Multibinder<RedisFieldDecoder<?>> fieldDecoderBinder = Multibinder.newSetBinder(binder, new TypeLiteral<RedisFieldDecoder<?>>() {});
        fieldDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }
}
