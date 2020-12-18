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
package com.facebook.presto.server.thrift.codec;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.internal.coercion.FromThrift;
import com.facebook.drift.codec.internal.coercion.ToThrift;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import org.joda.time.DateTime;

import javax.inject.Inject;

public class DateTimeToEpochMillisThriftCodec
        implements ThriftCodec<DateTime>
{
    @Inject
    public DateTimeToEpochMillisThriftCodec(ThriftCatalog thriftCatalog)
    {
        thriftCatalog.addDefaultCoercions(getClass());
    }

    @Override
    public ThriftType getType()
    {
        return new ThriftType(ThriftType.I64, DateTime.class);
    }

    @Override
    public DateTime read(TProtocolReader protocol)
            throws Exception
    {
        return longToDateTime(protocol.readI64());
    }

    @Override
    public void write(DateTime dateTime, TProtocolWriter protocol)
            throws Exception
    {
        protocol.writeI64(dateTimeToLong(dateTime));
    }

    @FromThrift
    public static DateTime longToDateTime(long instant)
    {
        return new DateTime(instant);
    }

    @ToThrift
    public static long dateTimeToLong(DateTime dateTime)
    {
        return dateTime.getMillis();
    }
}
