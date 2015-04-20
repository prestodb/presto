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
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class DateFormatParser
{
    private DateFormatParser()
    {
    }

    public static DateTimeFormatter createDateTimeFormatter(String format)
    {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        for (Token token : tokenize(format)) {
            switch (token.getType()) {
                case DateFormat.TEXT:
                    builder.appendLiteral(token.getText());
                    break;
                case DateFormat.DD:
                    builder.appendDayOfMonth(2);
                    break;
                case DateFormat.HH24:
                    builder.appendHourOfDay(2);
                    break;
                case DateFormat.HH:
                    builder.appendHourOfHalfday(2);
                    break;
                case DateFormat.MI:
                    builder.appendMinuteOfHour(2);
                    break;
                case DateFormat.MM:
                    builder.appendMonthOfYear(2);
                    break;
                case DateFormat.SS:
                    builder.appendSecondOfMinute(2);
                    break;
                case DateFormat.YY:
                    builder.appendTwoDigitYear(2050);
                    break;
                case DateFormat.YYYY:
                    builder.appendYear(4, 4);
                    break;
                case DateFormat.UNRECOGNIZED:
                default:
                    throw new PrestoException(
                            StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                            String.format("Failed to tokenize string [%s] at offset [%d]", token.getText(), token.getCharPositionInLine()));
            }
        }

        try {
            return builder.toFormatter();
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
