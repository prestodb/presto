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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.AbstractIterator;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Character.isLetterOrDigit;
import static java.lang.Character.isWhitespace;
import static java.lang.String.format;

public class JsonPathTokenizer
        extends AbstractIterator<String>
{
    private static final char QUOTE = '\"';
    private static final char DOT = '.';
    private static final char OPEN_BRACKET = '[';
    private static final char CLOSE_BRACKET = ']';
    private static final char UNICODE_CARET = '\u2038';

    private final String path;
    private int index;

    public JsonPathTokenizer(String path)
    {
        this.path = checkNotNull(path, "path is null");

        // skip the start token
        match('$');
    }

    @Override
    protected String computeNext()
    {
        skipWhitespace();
        if (!hasNextCharacter()) {
            return endOfData();
        }

        if (tryMatch(DOT)) {
            return matchUnquotedToken();
        }

        if (tryMatch(OPEN_BRACKET)) {
            String token = tryMatch(QUOTE) ? matchQuotedToken() : matchUnquotedToken();

            match(CLOSE_BRACKET);
            return token;
        }

        throw invalidJsonPath();
    }

    private String matchUnquotedToken()
    {
        skipWhitespace();

        // seek until we see a special character or whitespace
        int start = index;
        while (hasNextCharacter() && isLetterOrDigit(peekCharacter())) {
            nextCharacter();
        }
        int end = index;

        String token = path.substring(start, end);

        // an empty unquoted token is not allowed
        if (token.isEmpty()) {
            throw invalidJsonPath();
        }

        return token;
    }

    private String matchQuotedToken()
    {
        // quote has already been matched

        // seek until we see the close quote
        int start = index;
        while (hasNextCharacter() && peekCharacter() != QUOTE) {
            nextCharacter();
        }
        int end = index;

        String token = path.substring(start, end);

        match(QUOTE);
        return token;
    }

    private boolean hasNextCharacter()
    {
        return index < path.length();
    }

    private void match(char expected)
    {
        if (!tryMatch(expected)) {
            throw invalidJsonPath();
        }
    }

    private boolean tryMatch(char expected)
    {
        skipWhitespace();

        if (peekCharacter() != expected) {
            return false;
        }
        index++;
        return true;
    }

    private void skipWhitespace()
    {
        while (hasNextCharacter() && isWhitespace(peekCharacter())) {
            index++;
        }
    }

    private void nextCharacter()
    {
        index++;
    }

    private char peekCharacter()
    {
        return path.charAt(index);
    }

    public PrestoException invalidJsonPath()
    {
        return new PrestoException(INVALID_FUNCTION_ARGUMENT.toErrorCode(), format("Invalid JSON path: '%s'", path));
    }

    @Override
    public String toString()
    {
        return path.substring(0, index) + UNICODE_CARET + path.substring(index);
    }
}
