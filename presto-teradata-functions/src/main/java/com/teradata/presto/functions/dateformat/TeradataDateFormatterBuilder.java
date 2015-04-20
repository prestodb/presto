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
package com.teradata.presto.functions.dateformat;

import com.facebook.presto.spi.PrestoException;
import com.teradata.presto.functions.dateformat.tokens.DDToken;
import com.teradata.presto.functions.dateformat.tokens.MMToken;
import com.teradata.presto.functions.dateformat.tokens.TextToken;
import com.teradata.presto.functions.dateformat.tokens.YYYYToken;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.text.ParseException;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class TeradataDateFormatterBuilder
{
    private DateFormatLexer lexer;

    public TeradataDateFormatterBuilder()
    {
        lexer = DateFormatLexer.builder()
                .addTextToken("-")
                .addTextToken("/")
                .addTextToken(",")
                .addTextToken(".")
                .addTextToken(";")
                .addTextToken(":")
                .addToken(new YYYYToken())
                .addToken(new MMToken())
                .addToken(new DDToken())
                .build();
    }

    public DateTimeFormatter createDateTimeFormatter(String format) throws ParseException
    {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        for (Token token : lexer.tokenize(format)) {
            token.appendTo(builder);
        }

        try {
            return builder.toFormatter();
        }
        catch (UnsupportedOperationException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
