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
import com.facebook.presto.teradata.functions.dateformat.tokens.HH24Token;
import com.facebook.presto.teradata.functions.dateformat.tokens.DDToken;
import com.facebook.presto.teradata.functions.dateformat.tokens.HHToken;
import com.facebook.presto.teradata.functions.dateformat.tokens.MIToken;
import com.facebook.presto.teradata.functions.dateformat.tokens.MMToken;
import com.facebook.presto.teradata.functions.dateformat.tokens.SSToken;
import com.facebook.presto.teradata.functions.dateformat.tokens.YYToken;
import com.facebook.presto.teradata.functions.dateformat.tokens.YYYYToken;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class TeradataDateFormatterBuilder
{
    private DateFormatParser parser;

    public TeradataDateFormatterBuilder()
    {
        parser = DateFormatParser.builder()
                .add(new YYYYToken())
                .add(new YYToken())
                .add(new MMToken())
                .add(new DDToken())
                .add(new HH24Token())
                .add(new HHToken())
                .add(new MIToken())
                .add(new SSToken())
                .build();
    }

    public DateTimeFormatter createDateTimeFormatter(String format)
    {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        for (DateToken dateToken : parser.tokenize(format)) {
            dateToken.appendTo(builder);
        }

        try {
            return builder.toFormatter();
        }
        catch (UnsupportedOperationException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
