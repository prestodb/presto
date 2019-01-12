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
package io.prestosql.plugin.redis;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import io.prestosql.decoder.DispatchingRowDecoderFactory;
import io.prestosql.decoder.RowDecoderFactory;
import io.prestosql.decoder.avro.AvroRowDecoder;
import io.prestosql.decoder.avro.AvroRowDecoderFactory;
import io.prestosql.decoder.csv.CsvRowDecoder;
import io.prestosql.decoder.csv.CsvRowDecoderFactory;
import io.prestosql.decoder.dummy.DummyRowDecoder;
import io.prestosql.decoder.dummy.DummyRowDecoderFactory;
import io.prestosql.decoder.json.JsonRowDecoder;
import io.prestosql.decoder.json.JsonRowDecoderFactory;
import io.prestosql.decoder.raw.RawRowDecoder;
import io.prestosql.decoder.raw.RawRowDecoderFactory;
import io.prestosql.plugin.redis.decoder.hash.HashRedisRowDecoder;
import io.prestosql.plugin.redis.decoder.hash.HashRedisRowDecoderFactory;
import io.prestosql.plugin.redis.decoder.zset.ZsetRedisRowDecoder;
import io.prestosql.plugin.redis.decoder.zset.ZsetRedisRowDecoderFactory;

import static com.google.inject.Scopes.SINGLETON;

/**
 * Redis decoder specific module. Installs the registry and all known decoder submodules.
 */
public class RedisDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        MapBinder<String, RowDecoderFactory> decoderFactoriesByName = MapBinder.newMapBinder(binder, String.class, RowDecoderFactory.class);
        decoderFactoriesByName.addBinding(DummyRowDecoder.NAME).to(DummyRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(CsvRowDecoder.NAME).to(CsvRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(JsonRowDecoder.NAME).to(JsonRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(RawRowDecoder.NAME).to(RawRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(ZsetRedisRowDecoder.NAME).to(ZsetRedisRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(HashRedisRowDecoder.NAME).to(HashRedisRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(AvroRowDecoder.NAME).to(AvroRowDecoderFactory.class).in(SINGLETON);

        binder.bind(DispatchingRowDecoderFactory.class).in(SINGLETON);
    }
}
