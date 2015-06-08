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
import com.facebook.presto.teradata.functions.DateFormatLexer;
import com.facebook.presto.teradata.functions.dateformat.tokens.TextToken;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.Token;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

/*
 * Priority of tokens is determined by order of adding them
 */
public class DateFormatParser
{
    private final Map<Integer, DateToken> dateTokens;

    private DateFormatParser(Map<Integer, DateToken> dateTokens)
    {
        this.dateTokens = dateTokens;
    }

    public static DateFormatParserBuilder builder()
    {
        return new DateFormatParserBuilder();
    }

    public List<DateToken> tokenize(String string)
    {
        DateFormatLexer lexer = new com.facebook.presto.teradata.functions.DateFormatLexer(new ANTLRInputStream(string));
        return lexer.getAllTokens().stream()
                .map(this::getDateToken)
                .collect(Collectors.toList());
    }

    private DateToken getDateToken(Token token)
    {
        if (token.getType() == DateFormatLexer.TEXT) {
            return new TextToken(token.getText());
        }
        else if (token.getType() == DateFormatLexer.UNRECOGNIZED || !dateTokens.containsKey(token.getType())) {
            throw new PrestoException(
                    StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                    String.format("Failed to tokenize string [%s] at offset [%d]", token.getText(), token.getCharPositionInLine()));
        }
        return dateTokens.get(token.getType());
    }

    public static class DateFormatParserBuilder
    {
        private Map<Integer, DateToken> dateTokens = new HashMap<>();

        public DateFormatParser build()
        {
            return new DateFormatParser(dateTokens);
        }

        public DateFormatParserBuilder add(DateToken dateToken)
        {
            Integer key = dateToken.antlrToken();
            if (dateTokens.containsKey(key)) {
                throw new PrestoException(
                        StandardErrorCode.INTERNAL_ERROR,
                        String.format("Token [%d] is already registered", key));
            }
            dateTokens.put(key, dateToken);
            return this;
        }

        public DateFormatParserBuilder add(String text)
        {
            return add(new TextToken(text));
        }
    }
}
