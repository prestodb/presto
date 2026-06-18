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
package com.facebook.presto.teradata.functions.dateformat;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.teradata.functions.DateFormat;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.Token;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.time.format.SignStyle.NOT_NEGATIVE;
import static java.time.temporal.ChronoField.AMPM_OF_DAY;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_AMPM;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

public class DateFormatParser
{
    public enum Mode
    {
        // Do not require leading zero for parsing two position date fields (MM, DD, HH, HH24, MI, SS)
        // E.g. "to_timestamp('1988/4/8 2:3:4','yyyy/mm/dd hh24:mi:ss')"
        PARSER(1),

        // Add leading zero for formatting single valued two position date fields (MM, DD, HH, HH24, MI, SS)
        // E.g. "to_char(TIMESTAMP '1988-4-8 2:3:4','yyyy/mm/dd hh24:mi:ss')" evaluates to "1988/04/08 02:03:04"
        FORMATTER(2);

        private final int minTwoPositionFieldWidth;

        public int getMinTwoPositionFieldWidth()
        {
            return minTwoPositionFieldWidth;
        }

        Mode(int value)
        {
            this.minTwoPositionFieldWidth = value;
        }
    }

    private DateFormatParser()
    {
    }

    public static DateTimeFormatter createDateTimeFormatter(String format, Mode mode)
    {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        boolean formatContainsHourOfAMPM = false;
        for (Token token : tokenize(format)) {
            switch (token.getType()) {
                case DateFormat.TEXT:
                    builder.appendLiteral(token.getText());
                    break;
                case DateFormat.DD:
                    builder.appendValue(DAY_OF_MONTH, mode.getMinTwoPositionFieldWidth(), 2, NOT_NEGATIVE);
                    break;
                case DateFormat.HH24:
                    builder.appendValue(HOUR_OF_DAY, mode.getMinTwoPositionFieldWidth(), 2, NOT_NEGATIVE);
                    break;
                case DateFormat.HH:
                    builder.appendValue(HOUR_OF_AMPM, mode.getMinTwoPositionFieldWidth(), 2, NOT_NEGATIVE);
                    formatContainsHourOfAMPM = true;
                    break;
                case DateFormat.MI:
                    builder.appendValue(MINUTE_OF_HOUR, mode.getMinTwoPositionFieldWidth(), 2, NOT_NEGATIVE);
                    break;
                case DateFormat.MM:
                    builder.appendValue(MONTH_OF_YEAR, mode.getMinTwoPositionFieldWidth(), 2, NOT_NEGATIVE);
                    break;
                case DateFormat.SS:
                    builder.appendValue(SECOND_OF_MINUTE, mode.getMinTwoPositionFieldWidth(), 2, NOT_NEGATIVE);
                    break;
                case DateFormat.YY:
                    builder.appendValueReduced(YEAR, 2, 2, 2000);
                    break;
                case DateFormat.YYYY:
                    builder.appendValue(YEAR, 4);
                    break;
                case DateFormat.UNRECOGNIZED:
                default:
                    throw new PrestoException(
                            StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                            String.format("Failed to tokenize string [%s] at offset [%d]", token.getText(), token.getCharPositionInLine()));
            }
        }
        try {
            // Append default values(0) for time fields(HH24, HH, MI, SS) because JSR-310 does not accept bare Date value as DateTime

            if (formatContainsHourOfAMPM) {
                // At the moment format does not allow to include AM/PM token, thus it was never possible to specify PM hours using 'HH' token in format
                // Keep existing behaviour by defaulting to 0(AM) for AMPM_OF_DAY if format string contains 'HH'
                builder.parseDefaulting(HOUR_OF_AMPM, 0)
                        .parseDefaulting(AMPM_OF_DAY, 0);
            }
            else {
                builder.parseDefaulting(HOUR_OF_DAY, 0);
            }

            return builder.parseDefaulting(MINUTE_OF_HOUR, 0)
                    .parseDefaulting(SECOND_OF_MINUTE, 0)
                    .toFormatter();
        }
        catch (UnsupportedOperationException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    public static List<? extends Token> tokenize(String format)
    {
        DateFormat lexer = new com.facebook.presto.teradata.functions.DateFormat(new ANTLRInputStream(format));
        return lexer.getAllTokens();
    }
}
