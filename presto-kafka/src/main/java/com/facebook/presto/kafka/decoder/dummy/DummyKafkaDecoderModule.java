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
package com.facebook.presto.kafka.decoder.dummy;

import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindFieldDecoder;
import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindRowDecoder;

/**
 * Guice module for the 'dummy' decoder. See {@link com.facebook.presto.kafka.decoder.dummy.DummyKafkaRowDecoder} for an explanation.
 */
public class DummyKafkaDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindRowDecoder(binder, DummyKafkaRowDecoder.class);

        bindFieldDecoder(binder, DummyKafkaFieldDecoder.class);
    }
}
