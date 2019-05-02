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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

//
// A timestamp is stored as milliseconds from 1970-01-01T00:00:00 UTC.  When performing calculations
// on a timestamp the client's time zone must be taken into account.
//
public final class TimestampType
        extends AbstractLongType
{
    public static final TimestampType TIMESTAMP = new TimestampType();

    private TimestampType()
    {
        super(parseTypeSignature(StandardTypes.TIMESTAMP));
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (session.isLegacyTimestamp()) {
            return new SqlTimestamp(block.getLong(position, 0), session.getTimeZoneKey());
        }
        else {
            return new SqlTimestamp(block.getLong(position, 0));
        }
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == TIMESTAMP;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
