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
package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.decoder.csv.CsvKafkaDecoderModule;
import com.facebook.presto.kafka.decoder.dummy.DummyKafkaDecoderModule;
import com.facebook.presto.kafka.decoder.json.JsonKafkaDecoderModule;
import com.facebook.presto.kafka.decoder.raw.RawKafkaDecoderModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

/**
 * Kafka decoder specific module. Installs the registry and all known decoder submodules.
 */
public class KafkaDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(KafkaDecoderRegistry.class).in(Scopes.SINGLETON);

        binder.install(new DummyKafkaDecoderModule());
        binder.install(new CsvKafkaDecoderModule());
        binder.install(new JsonKafkaDecoderModule());
        binder.install(new RawKafkaDecoderModule());
    }

    public static void bindRowDecoder(Binder binder, Class<? extends KafkaRowDecoder> decoderClass)
    {
        Multibinder<KafkaRowDecoder> rowDecoderBinder = Multibinder.newSetBinder(binder, KafkaRowDecoder.class);
        rowDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }

    public static void bindFieldDecoder(Binder binder, Class<? extends KafkaFieldDecoder<?>> decoderClass)
    {
        Multibinder<KafkaFieldDecoder<?>> fieldDecoderBinder = Multibinder.newSetBinder(binder, new TypeLiteral<KafkaFieldDecoder<?>>() {});
        fieldDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }
}
