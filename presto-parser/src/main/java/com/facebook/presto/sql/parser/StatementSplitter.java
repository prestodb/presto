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
package com.facebook.presto.sql.parser;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenSource;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class StatementSplitter
{
    private final List<Statement> completeStatements;
    private final String partialStatement;

    public StatementSplitter(String sql)
    {
        this(sql, ImmutableSet.of(";"));
    }

    public StatementSplitter(String sql, Set<String> delimiters)
    {
        TokenSource tokens = getLexer(sql, delimiters);
        ImmutableList.Builder<Statement> list = ImmutableList.builder();
        StringBuilder sb = new StringBuilder();
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                break;
            }
            if (token.getType() == StatementLexer.TERMINATOR) {
                String statement = sb.toString().trim();
                if (!statement.isEmpty()) {
                    list.add(new Statement(statement, token.getText()));
                }
                sb = new StringBuilder();
            }
            else {
                sb.append(getTokenText(token));
            }
        }
        this.completeStatements = list.build();
        this.partialStatement = sb.toString().trim();
    }

    public List<Statement> getCompleteStatements()
    {
        return completeStatements;
    }

    public String getPartialStatement()
    {
        return partialStatement;
    }

    public static String squeezeStatement(String sql)
    {
        TokenSource tokens = getLexer(sql, ImmutableSet.<String>of());
        StringBuilder sb = new StringBuilder();
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                break;
            }
            if (token.getType() == StatementLexer.WS) {
                sb.append(' ');
            }
            else {
                sb.append(getTokenText(token));
            }
        }
        return sb.toString().trim();
    }

    public static boolean isEmptyStatement(String sql)
    {
        TokenSource tokens = getLexer(sql, ImmutableSet.<String>of());
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                return true;
            }
            if (token.getChannel() != Token.HIDDEN_CHANNEL) {
                return false;
            }
        }
    }

    private static String getTokenText(Token token)
    {
        switch (token.getType()) {
            case StatementLexer.STRING:
                return "'" + token.getText().replace("'", "''") + "'";
            case StatementLexer.QUOTED_IDENT:
                return "\"" + token.getText().replace("\"", "\"\"") + "\"";
            case StatementLexer.BACKQUOTED_IDENT:
                return "`" + token.getText().replace("`", "``") + "`";
        }
        return token.getText();
    }

    private static TokenSource getLexer(String sql, Set<String> terminators)
    {
        checkNotNull(sql, "sql is null");
        CharStream stream = new CaseInsensitiveStream(new ANTLRStringStream(sql));
        return new ErrorHandlingLexer(stream, terminators);
    }

    /**
     * ANTLR has two options for handling lexer errors: attempt to ignore the
     * bad input and continue, or abort lexing by throwing an exception.
     * However, we need to tokenize all input even if it's invalid and
     * reconstruct the original input from the tokens, but neither of those
     * options allows for that. So, we create an alternative: convert lexer
     * errors into a special token, which preserves all of the original input
     * text. ANTLR doesn't provide a way to do this, so we create a copy of
     * the {@link org.antlr.runtime.Lexer#nextToken} method with the
     * appropriate modifications for creating the special error token.
     */
    private static class ErrorHandlingLexer
            extends StatementLexer
    {
        private final Set<String> terminators;

        public ErrorHandlingLexer(CharStream input, Set<String> terminators)
        {
            super(input);

            // allow identifier symbols, since illegal identifiers will be handled by the main parser
            setAllowedIdentifierSymbols(EnumSet.allOf(IdentifierSymbol.class));

            this.terminators = ImmutableSet.copyOf(checkNotNull(terminators, "terminators is null"));
        }

        @SuppressWarnings("ObjectEquality")
        @Override
        public Token nextToken()
        {
            while (true) {
                state.token = null;
                state.channel = Token.DEFAULT_CHANNEL;
                state.tokenStartCharIndex = input.index();
                state.tokenStartCharPositionInLine = input.getCharPositionInLine();
                state.tokenStartLine = input.getLine();
                state.text = null;

                if (input.LA(1) == CharStream.EOF) {
                    Token eof = new CommonToken(input, Token.EOF, Token.DEFAULT_CHANNEL, input.index(), input.index());
                    eof.setLine(getLine());
                    eof.setCharPositionInLine(getCharPositionInLine());
                    return eof;
                }

                for (String terminator : terminators) {
                    if (matches(terminator)) {
                        state.type = TERMINATOR;
                        emit();
                        return state.token;
                    }
                }

                try {
                    mTokens();
                    if (state.token == null) {
                        emit();
                    }
                    else if (state.token == Token.SKIP_TOKEN) {
                        continue;
                    }
                    return state.token;
                }
                catch (RecognitionException re) {
                    input.consume();
                    state.type = LEXER_ERROR;
                    emit();
                    return state.token;
                }
            }
        }

        private boolean matches(String s)
        {
            for (int i = 0; i < s.length(); i++) {
                if (input.LA(i + 1) != s.charAt(i)) {
                    return false;
                }
            }
            for (int i = 0; i < s.length(); i++) {
                input.consume();
            }
            return true;
        }
    }

    public static class Statement
    {
        private final String statement;
        private final String terminator;

        public Statement(String statement, String terminator)
        {
            this.statement = checkNotNull(statement, "statement is null");
            this.terminator = checkNotNull(terminator, "terminator is null");
        }

        public String statement()
        {
            return statement;
        }

        public String terminator()
        {
            return terminator;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            Statement o = (Statement) obj;
            return Objects.equal(statement, o.statement) &&
                    Objects.equal(terminator, o.terminator);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(statement, terminator);
        }

        @Override
        public String toString()
        {
            return statement + terminator;
        }
    }
}
