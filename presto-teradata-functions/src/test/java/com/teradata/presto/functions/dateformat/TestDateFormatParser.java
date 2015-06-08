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
import com.teradata.presto.functions.dateformat.tokens.MMToken;
import com.teradata.presto.functions.dateformat.tokens.TextToken;
import com.teradata.presto.functions.dateformat.tokens.YYToken;
import com.teradata.presto.functions.dateformat.tokens.YYYYToken;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestDateFormatParser
{
    private static DateFormatParser parser = DateFormatParser.builder()
            .add(new YYYYToken())
            .add(new YYToken())
            .add(new MMToken())
            .build();

    private static List<DateToken> tokenize(String format)
    {
        return parser.tokenize(format);
    }

    @Test
    public void testLexer() throws Exception
    {
        String format = "yyyy mm";
        assertEquals(tokenize(format), Arrays.asList(new YYYYToken(), new TextToken(" "), new MMToken()));
    }

    @Test
    public void testGreedinessLongFirst()
    {
        assertEquals(1, tokenize("yy").size());
        assertEquals(1, tokenize("yyyy").size());
        assertEquals(2, tokenize("yyyyyy").size());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidToken()
    {
        tokenize("ala");
    }
}
