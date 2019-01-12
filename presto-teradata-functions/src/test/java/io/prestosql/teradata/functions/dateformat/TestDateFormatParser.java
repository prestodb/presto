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
package io.prestosql.teradata.functions.dateformat;

import io.prestosql.spi.PrestoException;
import io.prestosql.teradata.functions.DateFormat;
import org.antlr.v4.runtime.Token;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.Test;

import java.util.stream.Collectors;

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
        DateFormatParser.createDateTimeFormatter("ala");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidTokenCreate2()
    {
        DateFormatParser.createDateTimeFormatter("yyym/mm/dd");
    }

    @Test
    public void testCreateDateTimeFormatter()
    {
        DateTimeFormatter formatter = DateFormatParser.createDateTimeFormatter("yyyy/mm/dd");
        assertEquals(formatter.parseDateTime("1988/04/08"), new DateTime(1988, 4, 8, 0, 0));
    }
}
