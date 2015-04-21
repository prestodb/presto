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

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

import com.facebook.presto.spi.PrestoException;
import com.teradata.presto.functions.dateformat.tokens.TextToken;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

/*
 * Priority of tokens is determined by order of adding them
 */
public class DateFormatLexer
{
    private Map<Character, List<Token>> tokensByFirstCharMap;

    public DateFormatLexer(Map<Character, List<Token>> tokens)
    {
        this.tokensByFirstCharMap = tokens;
    }

    public static DateFormatLexerBuilder builder()
    {
        return new DateFormatLexerBuilder();
    }

    public List<Token> tokenize(String string)
    {
        return tokenize(string, 0);
    }

    private List<Token> tokenize(String string, int offset)
    {
        List<Token> result = new ArrayList<>();

        while (offset < string.length()) {
            boolean noTokenFound = true;
            for (Token token : getPossibleTokens(string, offset)) {
                if (string.startsWith(token.representation(), offset)) {
                    result.add(token);
                    offset += token.representation().length();
                    noTokenFound = false;
                    break;
                }
            }

            if (noTokenFound) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        String.format("Failed to tokenize string [%s] at offset [%d]", string, offset));
            }
        }
        return result;
    }

    private List<Token> getPossibleTokens(String string, int offset)
    {
        Character firstChar = string.charAt(offset);
        if (!tokensByFirstCharMap.containsKey(firstChar)) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    String.format(
                            "No tokens starts with character [%c] in string [%s] at offset [%d]",
                            firstChar,
                            string,
                            offset));
        }
        return tokensByFirstCharMap.get(firstChar);
    }

    public static class DateFormatLexerBuilder
    {
        private Map<Character, List<Token>> tokens = new HashMap<>();

        public DateFormatLexer build()
        {
            return new DateFormatLexer(tokens);
        }

        public DateFormatLexerBuilder addToken(Token token)
        {
            Character key = token.representation().charAt(0);
            if (!tokens.containsKey(key)) {
                tokens.put(key, new ArrayList<>());
            }
            tokens.get(key).add(token);
            return this;
        }

        public DateFormatLexerBuilder addTextToken(String text)
        {
            return addToken(new TextToken(text));
        }
    }
}
