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
import com.facebook.presto.teradata.functions.DateFormat;
import org.antlr.v4.runtime.Token;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;

import static com.facebook.presto.teradata.functions.dateformat.DateFormatParser.Mode.FORMATTER;
import static com.facebook.presto.teradata.functions.dateformat.DateFormatParser.Mode.PARSER;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestDateFormatParser
{
    @Test
    public void testTokenize()
    {
        assertEquals(
                DateFormatParser.tokenize("yyyy mm").stream().map(Token::getType).collect(Collectors.toList()),
                asList(DateFormat.YYYY, DateFormat.TEXT, DateFormat.MM));
    }

    @Test
    public void testGreedinessLongFirst()
    {
        assertEquals(1, DateFormatParser.tokenize("yy").size());
        assertEquals(1, DateFormatParser.tokenize("yyyy").size());
        assertEquals(2, DateFormatParser.tokenize("yyyyyy").size());
    }

    @Test
    public void testInvalidTokenTokenize()
    {
        assertEquals(
                DateFormatParser.tokenize("ala").stream().map(Token::getType).collect(Collectors.toList()),
                asList(DateFormat.UNRECOGNIZED, DateFormat.UNRECOGNIZED, DateFormat.UNRECOGNIZED));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidTokenCreate1()
    {
        DateFormatParser.createDateTimeFormatter("ala", FORMATTER);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidTokenCreate2()
    {
        DateFormatParser.createDateTimeFormatter("yyym/mm/dd", FORMATTER);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testParserInvalidTokenCreate1()
    {
        DateFormatParser.createDateTimeFormatter("ala", PARSER);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testParserInvalidTokenCreate2()
    {
        DateFormatParser.createDateTimeFormatter("yyym/mm/dd", PARSER);
    }

    @Test
    public void testCreateDateTimeFormatter()
    {
        DateTimeFormatter formatter = DateFormatParser.createDateTimeFormatter("yyyy/mm/dd", FORMATTER);
        assertEquals(formatter.format(LocalDateTime.of(1988, 4, 8, 0, 0)), "1988/04/08");
    }

    @Test
    public void testCreateDateTimeParser()
    {
        DateTimeFormatter formatter = DateFormatParser.createDateTimeFormatter("yyyy/mm/dd", PARSER);
        assertEquals(LocalDateTime.parse("1988/04/08", formatter), LocalDateTime.of(1988, 4, 8, 0, 0));
    }
}
