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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.semantic.SemanticType;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;

//
// A time is stored as milliseconds from midnight on 1970-01-01T00:00:00 in the time zone of the session.
// When performing calculations on a time the client's time zone must be taken into account.
//
public final class TimeType
        extends AbstractLongType
{
    public static final TimeType TIME = new TimeType();
    public static final SemanticType TIME_TYPE = SemanticType.from(TIME);

    private TimeType()
    {
        super(parseTypeSignature(StandardTypes.TIME));
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (properties.isLegacyTimestamp()) {
            return new SqlTime(block.getLong(position), properties.getTimeZoneKey());
        }
        else {
            return new SqlTime(block.getLong(position));
        }
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == TIME;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
