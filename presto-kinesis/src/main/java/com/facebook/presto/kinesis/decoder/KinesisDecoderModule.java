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
package com.facebook.presto.kinesis.decoder;

import com.facebook.presto.kinesis.decoder.csv.CsvKinesisDecoderModule;
import com.facebook.presto.kinesis.decoder.dummy.DummyKinesisDecoderModule;
import com.facebook.presto.kinesis.decoder.json.JsonKinesisDecoderModule;
import com.facebook.presto.kinesis.decoder.raw.RawKinesisDecoderModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

/**
 *
 * Kinesis Decoder implementation of Injection Module interface. Binds all the Row and column decoder modules.
 *
 */
public class KinesisDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(KinesisDecoderRegistry.class).in(Scopes.SINGLETON);

        binder.install(new DummyKinesisDecoderModule());
        binder.install(new CsvKinesisDecoderModule());
        binder.install(new JsonKinesisDecoderModule());
        binder.install(new RawKinesisDecoderModule());
    }

    public static void bindRowDecoder(Binder binder, Class<? extends KinesisRowDecoder> decoderClass)
    {
        Multibinder<KinesisRowDecoder> rowDecoderBinder = Multibinder.newSetBinder(binder, KinesisRowDecoder.class);
        rowDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }

    public static void bindFieldDecoder(Binder binder, Class<? extends KinesisFieldDecoder<?>> decoderClass)
    {
        Multibinder<KinesisFieldDecoder<?>> fieldDecoderBinder = Multibinder.newSetBinder(binder, new TypeLiteral<KinesisFieldDecoder<?>>() {});
        fieldDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }
}
