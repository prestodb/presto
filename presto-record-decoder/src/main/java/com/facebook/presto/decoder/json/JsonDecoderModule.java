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
package com.facebook.presto.decoder.json;

import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.presto.decoder.DecoderModule.bindFieldDecoder;
import static com.facebook.presto.decoder.DecoderModule.bindRowDecoder;

/**
 * Guice module for the Json decoder module. This is the most mature (best tested) topic decoder.
 * <p>
 * Besides the default field decoder for all the values, it also supports a number of decoders for
 * timestamp specific information. These decoders can be selected with the <tt>dataFormat</tt> field.
 * <p>
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
public class JsonDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindRowDecoder(binder, JsonRowDecoder.class);

        bindFieldDecoder(binder, JsonFieldDecoder.class);
        bindFieldDecoder(binder, ISO8601JsonFieldDecoder.class);
        bindFieldDecoder(binder, RFC2822JsonFieldDecoder.class);
        bindFieldDecoder(binder, SecondsSinceEpochJsonFieldDecoder.class);
        bindFieldDecoder(binder, MillisecondsSinceEpochJsonFieldDecoder.class);
        bindFieldDecoder(binder, CustomDateTimeJsonFieldDecoder.class);
    }
}
