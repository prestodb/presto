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
// A date is stored as days from 1970-01-01.
//
// Note: when dealing with a java.sql.Date it is important to remember that the value is stored
// as the number of milliseconds from 1970-01-01T00:00:00 in UTC but time must be midnight in
// the local time zone.  This mean when converting between a java.sql.Date and this
// type, the time zone offset must be added or removed to keep the time at midnight in UTC.
//
public final class DateType
        extends AbstractIntType
{
    public static final DateType DATE = new DateType();
    public static final SemanticType DATE_TYPE = SemanticType.from(DATE);

    private DateType()
    {
        super(parseTypeSignature(StandardTypes.DATE));
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        int days = block.getInt(position);
        return new SqlDate(days);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == DATE;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
