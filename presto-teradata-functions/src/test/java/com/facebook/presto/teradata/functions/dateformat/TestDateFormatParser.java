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
import com.facebook.presto.teradata.functions.dateformat.tokens.YYYYToken;
import com.facebook.presto.teradata.functions.dateformat.tokens.MMToken;
import com.facebook.presto.teradata.functions.dateformat.tokens.TextToken;
import com.facebook.presto.teradata.functions.dateformat.tokens.YYToken;
import org.testng.annotations.Test;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestDateFormatParser
{
    private static DateFormatParser parser = DateFormatParser.builder()
            .add(new YYYYToken())
            .add(new YYToken())
            .add(new MMToken())
            .build();

    @Test
    public void testLexer() throws Exception
    {
        String format = "yyyy mm";
        assertEquals(parser.tokenize(format), asList(new YYYYToken(), new TextToken(" "), new MMToken()));
    }

    @Test
    public void testGreedinessLongFirst()
    {
        assertEquals(1, parser.tokenize("yy").size());
        assertEquals(1, parser.tokenize("yyyy").size());
        assertEquals(2, parser.tokenize("yyyyyy").size());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidToken()
    {
        parser.tokenize("ala");
    }
}
