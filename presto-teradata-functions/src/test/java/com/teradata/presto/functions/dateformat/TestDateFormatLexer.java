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
import com.teradata.presto.functions.dateformat.tokens.TextToken;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

public class TestDateFormatLexer
{
    private DateFormatLexer yearLexer;

    @BeforeClass
    public void setUp()
    {
        yearLexer = DateFormatLexer.builder().add(token("yyyy")).build();
    }

    @Test
    public void testSimpleFormat()
    {
        List<Token> tokens = yearLexer.tokenize("yyyy");
        assertThat(tokens).containsExactly(token("yyyy"));
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
                .add(token("yyyy"))
                .add(token("mm"))
                .add(token("dd"))
                .add(token("-"))
                .add(token("/"))
                .build();

        List<Token> tokens = yearMonthDayLexer.tokenize("mm-dd/yyyy");
        assertThat(tokens).containsExactly(token("mm"), token("-"), token("dd"), token("/"), token("yyyy"));
    }

    @Test
    public void testGreedinessLongFirst()
    {
        DateFormatLexer lexer = DateFormatLexer.builder()
                .add(token("yyy"))
                .add(token("yy"))
                .add(token("y"))
                .build();

        assertThat(lexer.tokenize("y")).hasSize(1);
        assertThat(lexer.tokenize("yy")).hasSize(1);
        assertThat(lexer.tokenize("yyy")).hasSize(1);
        assertThat(lexer.tokenize("yyyy")).hasSize(2);
        assertThat(lexer.tokenize("yyyyy")).hasSize(2);
        assertThat(lexer.tokenize("yyyyyy")).hasSize(2);
        assertThat(lexer.tokenize("yyyyyyy")).hasSize(3);
    }

    @Test
    public void testGreedinessShortFirst()
    {
        DateFormatLexer lexer = DateFormatLexer.builder()
                .add(token("y"))
                .add(token("yy"))
                .add(token("yyy"))
                .build();

        assertThat(lexer.tokenize("y")).hasSize(1);
        assertThat(lexer.tokenize("yy")).hasSize(2);
        assertThat(lexer.tokenize("yyy")).hasSize(3);
    }

    private static Token token(String text)
    {
        return new TextToken(text);
    }
}
