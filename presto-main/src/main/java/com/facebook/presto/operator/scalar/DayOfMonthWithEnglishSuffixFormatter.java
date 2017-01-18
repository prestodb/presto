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
package com.facebook.presto.operator.scalar;

import com.google.common.collect.ImmutableMap;
import org.joda.time.Chronology;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePartial;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimeParserBucket;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.FormatUtils;

import java.io.IOException;
import java.io.Writer;
import java.util.Locale;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public final class DayOfMonthWithEnglishSuffixFormatter
        implements DateTimePrinter, DateTimeParser
{
    private static final DateTimeFieldType DAY_OF_MONTH = DateTimeFieldType.dayOfMonth();
    private static final Map<String, Integer> THREE_LETTER_PARSE_LOOKUP = new ImmutableMap.Builder<String, Integer>()
            .put("1st", 1)
            .put("2nd", 2)
            .put("3rd", 3)
            .put("4th", 4)
            .put("5th", 5)
            .put("6th", 6)
            .put("7th", 7)
            .put("8th", 8)
            .put("9th", 9)
            .build();
    private static final Map<String, Integer> FOUR_LETTER_PARSE_LOOKUP = new ImmutableMap.Builder<String, Integer>()
            .put("10th", 10)
            .put("11th", 11)
            .put("12th", 12)
            .put("13th", 13)
            .put("14th", 14)
            .put("15th", 15)
            .put("16th", 16)
            .put("17th", 17)
            .put("18th", 18)
            .put("19th", 19)
            .put("20th", 20)
            .put("21st", 21)
            .put("22nd", 22)
            .put("23rd", 23)
            .put("24th", 24)
            .put("25th", 25)
            .put("26th", 26)
            .put("27th", 27)
            .put("28th", 28)
            .put("29th", 29)
            .put("30th", 30)
            .put("31st", 31)
            .build();

    @Override
    public int estimatePrintedLength()
    {
        return 4;
    }

    private static String getSuffix(int day)
    {
        checkArgument(day > 0 && day <= 31, "%s is not a valid day of month", day);
        switch(day) {
            case 1:
            case 21:
            case 31:
                return "st";
            case 2:
            case 22:
                return "nd";
            case 3:
            case 23:
                return "rd";
            default:
                return "th";
        }
    }

    private void printTo(Appendable appendable, long instant, Chronology chrono) throws IOException
    {
        int day = DAY_OF_MONTH.getField(chrono).get(instant);
        FormatUtils.appendUnpaddedInteger(appendable, day);
        appendable.append(getSuffix(day));
    }

    @Override
    public void printTo(StringBuffer buf, long instant, Chronology chrono, int displayOffset, DateTimeZone displayZone, Locale locale)
    {
        try {
            printTo(buf, instant, chrono);
        }
        catch (IOException e) {
            // StringBuffer does not throw IOException
        }
    }

    @Override
    public void printTo(Writer out, long instant, Chronology chrono, int displayOffset, DateTimeZone displayZone, Locale locale)
            throws IOException
    {
        printTo(out, instant, chrono);
    }

    private void printTo(Appendable appendable, ReadablePartial partial) throws IOException
    {
        if (partial.isSupported(DAY_OF_MONTH)) {
            int day = partial.get(DAY_OF_MONTH);
            FormatUtils.appendUnpaddedInteger(appendable, day);
            appendable.append(getSuffix(day));
        }
        else {
            appendUnknownString(appendable, 3);
        }
    }

    @Override
    public void printTo(StringBuffer buf, ReadablePartial partial, Locale locale)
    {
        try {
            printTo(buf, partial);
        }
        catch (IOException ex) {
            // StringBuffer does not throw IOException
        }
    }

    @Override
    public void printTo(Writer out, ReadablePartial partial, Locale locale)
            throws IOException
    {
        printTo(out, partial);
    }

    private static void appendUnknownString(Appendable appendable, int len) throws IOException
    {
        for (int i = len; i >= 0; i--) {
            appendable.append('\ufffd');
        }
    }

    @Override
    public int estimateParsedLength()
    {
        return 4;
    }

    @Override
    public int parseInto(DateTimeParserBucket bucket, String text, int position)
    {
        if (text.length() - position < 3) {
            return ~position;
        }

        String token = text.substring(position, position + 3).toLowerCase();
        if (THREE_LETTER_PARSE_LOOKUP.containsKey(token)) {
            bucket.saveField(DAY_OF_MONTH, THREE_LETTER_PARSE_LOOKUP.get(token));
            return position + 3;
        }

        if (text.length() - position < 4) {
            return ~position;
        }

        token = text.substring(position, position + 4).toLowerCase();
        if (FOUR_LETTER_PARSE_LOOKUP.containsKey(token)) {
            bucket.saveField(DAY_OF_MONTH, FOUR_LETTER_PARSE_LOOKUP.get(token));
            return position + 4;
        }

        return ~position;
    }
}
