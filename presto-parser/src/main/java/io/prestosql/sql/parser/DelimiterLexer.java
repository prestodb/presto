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
package io.prestosql.sql.parser;

import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.LexerNoViableAltException;
import org.antlr.v4.runtime.Token;

import java.util.Set;

/**
 * This is a special-purpose lexer that can identify custom delimiters in addition to every other
 * token in the SQL grammar.
 * <p>
 * The code in nextToken() is a copy of the implementation in org.antlr.v4.runtime.Lexer, with a
 * bit added to match the token before the default behavior is invoked.
 */
class DelimiterLexer
        extends SqlBaseLexer
{
    private final Set<String> delimiters;

    public DelimiterLexer(CharStream input, Set<String> delimiters)
    {
        super(input);
        this.delimiters = ImmutableSet.copyOf(delimiters);
    }

    @Override
    public Token nextToken()
    {
        if (_input == null) {
            throw new IllegalStateException("nextToken requires a non-null input stream.");
        }

        // Mark start location in char stream so unbuffered streams are
        // guaranteed at least have text of current token
        int tokenStartMarker = _input.mark();
        try {
            outer:
            while (true) {
                if (_hitEOF) {
                    emitEOF();
                    return _token;
                }

                _token = null;
                _channel = Token.DEFAULT_CHANNEL;
                _tokenStartCharIndex = _input.index();
                _tokenStartCharPositionInLine = getInterpreter().getCharPositionInLine();
                _tokenStartLine = getInterpreter().getLine();
                _text = null;
                do {
                    _type = Token.INVALID_TYPE;
                    int ttype = -1;

                    // This entire method is copied from org.antlr.v4.runtime.Lexer, with the following bit
                    // added to match the delimiters before we attempt to match the token
                    boolean found = false;
                    for (String terminator : delimiters) {
                        if (match(terminator)) {
                            ttype = SqlBaseParser.DELIMITER;
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        try {
                            ttype = getInterpreter().match(_input, _mode);
                        }
                        catch (LexerNoViableAltException e) {
                            notifyListeners(e);        // report error
                            recover(e);
                            ttype = SKIP;
                        }
                    }

                    if (_input.LA(1) == IntStream.EOF) {
                        _hitEOF = true;
                    }
                    if (_type == Token.INVALID_TYPE) {
                        _type = ttype;
                    }
                    if (_type == SKIP) {
                        continue outer;
                    }
                }
                while (_type == MORE);
                if (_token == null) {
                    emit();
                }
                return _token;
            }
        }
        finally {
            // make sure we release marker after match or
            // unbuffered char stream will keep buffering
            _input.release(tokenStartMarker);
        }
    }

    private boolean match(String delimiter)
    {
        for (int i = 0; i < delimiter.length(); i++) {
            if (_input.LA(i + 1) != delimiter.charAt(i)) {
                return false;
            }
        }
        _input.seek(_input.index() + delimiter.length());
        return true;
    }
}
