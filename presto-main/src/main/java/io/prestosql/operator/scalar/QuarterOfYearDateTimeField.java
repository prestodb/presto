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
package io.prestosql.operator.scalar;

import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DurationField;
import org.joda.time.DurationFieldType;
import org.joda.time.field.DividedDateTimeField;
import org.joda.time.field.OffsetDateTimeField;
import org.joda.time.field.ScaledDurationField;

// Forked from org.elasticsearch.common.joda.Joda
public final class QuarterOfYearDateTimeField
        extends DateTimeFieldType
{
    private static final long serialVersionUID = -5677872459807379123L;

    private static final DurationFieldType QUARTER_OF_YEAR_DURATION_FIELD_TYPE = new QuarterOfYearDurationFieldType();

    public static final DateTimeFieldType QUARTER_OF_YEAR = new QuarterOfYearDateTimeField();

    private QuarterOfYearDateTimeField()
    {
        super("quarterOfYear");
    }

    @Override
    public DurationFieldType getDurationType()
    {
        return QUARTER_OF_YEAR_DURATION_FIELD_TYPE;
    }

    @Override
    public DurationFieldType getRangeDurationType()
    {
        return DurationFieldType.years();
    }

    @Override
    public DateTimeField getField(Chronology chronology)
    {
        return new OffsetDateTimeField(new DividedDateTimeField(new OffsetDateTimeField(chronology.monthOfYear(), -1), QUARTER_OF_YEAR, 3), 1);
    }

    private static class QuarterOfYearDurationFieldType
            extends DurationFieldType
    {
        private static final long serialVersionUID = -8167713675442491871L;

        public QuarterOfYearDurationFieldType()
        {
            super("quarters");
        }

        @Override
        public DurationField getField(Chronology chronology)
        {
            return new ScaledDurationField(chronology.months(), QUARTER_OF_YEAR_DURATION_FIELD_TYPE, 3);
        }
    }
}
