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
package com.facebook.presto.decoder;

import com.facebook.presto.decoder.csv.CsvDecoderModule;
import com.facebook.presto.decoder.dummy.DummyDecoderModule;
import com.facebook.presto.decoder.json.JsonDecoderModule;
import com.facebook.presto.decoder.raw.RawDecoderModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

/**
 * Default decoder module. Installs the registry and all known decoder submodules.
 */
public class DecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DecoderRegistry.class).in(Scopes.SINGLETON);

        binder.install(new DummyDecoderModule());
        binder.install(new CsvDecoderModule());
        binder.install(new JsonDecoderModule());
        binder.install(new RawDecoderModule());
    }

    public static void bindRowDecoder(Binder binder, Class<? extends RowDecoder> decoderClass)
    {
        Multibinder<RowDecoder> rowDecoderBinder = Multibinder.newSetBinder(binder, RowDecoder.class);
        rowDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }

    public static void bindFieldDecoder(Binder binder, Class<? extends FieldDecoder<?>> decoderClass)
    {
        Multibinder<FieldDecoder<?>> fieldDecoderBinder = Multibinder.newSetBinder(binder, new TypeLiteral<FieldDecoder<?>>() {});
        fieldDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }
}
