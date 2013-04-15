package com.facebook.presto.sql.parser;

import com.google.common.collect.ImmutableList;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenSource;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class StatementSplitter
{
    private final List<String> completeStatements;
    private final String partialStatement;

    public StatementSplitter(String sql)
    {
        TokenSource tokens = getLexer(checkNotNull(sql, "sql is null"));
        ImmutableList.Builder<String> list = ImmutableList.builder();
        StringBuilder sb = new StringBuilder();
        int index = 0;
        while (true) {
            Token token;
            try {
                token = tokens.nextToken();
                index = ((CommonToken) token).getStopIndex() + 1;
            }
            catch (ParsingException e) {
                sb.append(sql.substring(index));
                break;
            }
            if (token.getType() == Token.EOF) {
                break;
            }
            if (token.getType() == StatementLexer.SEMICOLON) {
                String statement = sb.toString().trim();
                if (!statement.isEmpty()) {
                    list.add(statement);
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

    public List<String> getCompleteStatements()
    {
        return completeStatements;
    }

    public String getPartialStatement()
    {
        return partialStatement;
    }

    public static String squeezeStatement(String sql)
    {
        TokenSource tokens = getLexer(checkNotNull(sql, "sql is null"));
        StringBuilder sb = new StringBuilder();
        int index = 0;
        while (true) {
            Token token;
            try {
                token = tokens.nextToken();
                index = ((CommonToken) token).getStopIndex() + 1;
            }
            catch (ParsingException e) {
                sb.append(sql.substring(index));
                break;
            }
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

    private static TokenSource getLexer(String sql)
    {
        return new ErrorHandlingLexer(new CaseInsensitiveStream(new ANTLRStringStream(sql)));
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
        public ErrorHandlingLexer(CharStream input)
        {
            super(input);
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
    }
}
