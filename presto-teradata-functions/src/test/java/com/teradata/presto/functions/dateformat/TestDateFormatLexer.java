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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestDateFormatLexer
{
    private DateFormatLexer yearLexer;

    @BeforeClass
    public void setUp()
    {
        yearLexer = DateFormatLexer.builder().add("yyyy").build();
    }

    @Test
    public void testSimpleFormat()
    {
        List<Token> tokens = yearLexer.tokenize("yyyy");
        assertEquals(tokens.size(), 1);
        assertEquals(tokens.get(0).representation(), "yyyy");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidToken1()
    {
        yearLexer.tokenize("yyy");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidToken2()
    {
        yearLexer.tokenize("ala");
    }

    @Test
    public void testYearMonthDay()
    {
        DateFormatLexer yearMonthDayLexer = DateFormatLexer.builder()
                .add("yyyy")
                .add("mm")
                .add("dd")
                .add("-")
                .add("/")
                .build();

        List<Token> tokens = yearMonthDayLexer.tokenize("mm-dd/yyyy");
        assertEquals(tokens.size(), 5);
        assertEquals(tokens.get(0).representation(), "mm");
        assertEquals(tokens.get(1).representation(), "-");
        assertEquals(tokens.get(2).representation(), "dd");
        assertEquals(tokens.get(3).representation(), "/");
        assertEquals(tokens.get(4).representation(), "yyyy");
    }

    @Test
    public void testGreedinessLongFirst()
    {
        DateFormatLexer lexer = DateFormatLexer.builder()
                .add("yyy")
                .add("yy")
                .add("y")
                .build();

        assertEquals(lexer.tokenize("y").size(), 1);
        assertEquals(lexer.tokenize("yy").size(), 1);
        assertEquals(lexer.tokenize("yyy").size(), 1);
        assertEquals(lexer.tokenize("yyyy").size(), 2);
        assertEquals(lexer.tokenize("yyyyy").size(), 2);
        assertEquals(lexer.tokenize("yyyyyy").size(), 2);
        assertEquals(lexer.tokenize("yyyyyyy").size(), 3);
    }

    @Test
    public void testGreedinessShortFirst()
    {
        DateFormatLexer lexer = DateFormatLexer.builder()
                .add("y")
                .add("yy")
                .add("yyy")
                .build();

        assertEquals(lexer.tokenize("y").size(), 1);
        assertEquals(lexer.tokenize("yy").size(), 2);
        assertEquals(lexer.tokenize("yyy").size(), 3);
    }
}
