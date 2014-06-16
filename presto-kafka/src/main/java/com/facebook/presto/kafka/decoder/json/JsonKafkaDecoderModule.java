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
package com.facebook.presto.kafka.decoder.json;

import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindFieldDecoder;
import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindRowDecoder;

/**
 * Guice module for the Json decoder module. This is the most mature (best tested) topic decoder.
 * <p/>
 * Besides the default field decoder for all the values, it also supports a number of decoders for
 * timestamp specific information. These decoders can be selected with the <tt>dataFormat</tt> field.
 * <p/>
 * <ul>
 * <li><tt>iso8601</tt> - decode the value of a json string field as an ISO8601 timestamp; returns a long value which can be mapped to a presto TIMESTAMP.</li>
 * <li><tt>rfc2822</tt> - decode the value of a json string field as an RFC 2822 compliant timestamp; returns a long value which can be mapped to a presto TIMESTAMP
 * (the twitter sample feed contains timestamps in this format).</li>
 * <li><tt>milliseconds-since-epoch</tt> - Interpret the value of a json string or number field as a long containing milliseconds since the beginning of the epoch.</li>
 * <li><tt>seconds-since-epoch</tt> - Interpret the value of a json string or number field as a long containing seconds since the beginning of the epoch.</li>
 * <li><tt>custom-date-time</tt> - Interpret the value of a json string field according to the {@link org.joda.time.format.DateTimeFormatter} formatting rules
 * given using the <tt>formatHint</tt> field.</li>
 * </ul>
 */
public class JsonKafkaDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindRowDecoder(binder, JsonKafkaRowDecoder.class);

        bindFieldDecoder(binder, JsonKafkaFieldDecoder.class);
        bindFieldDecoder(binder, ISO8601JsonKafkaFieldDecoder.class);
        bindFieldDecoder(binder, RFC2822JsonKafkaFieldDecoder.class);
        bindFieldDecoder(binder, SecondsSinceEpochJsonKafkaFieldDecoder.class);
        bindFieldDecoder(binder, MillisecondsSinceEpochJsonKafkaFieldDecoder.class);
        bindFieldDecoder(binder, CustomDateTimeJsonKafkaFieldDecoder.class);
    }
}
