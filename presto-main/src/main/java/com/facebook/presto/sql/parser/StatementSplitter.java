package com.facebook.presto.sql.parser;

import com.google.common.collect.ImmutableList;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonToken;
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
            catch (TokenizationException e) {
                sb.append(sql.substring(index));
                break;
            }
            if (token.getType() == Token.EOF) {
                break;
            }
            if (token.getType() == StatementLexer.SEMICOLON) {
                list.add(sb.toString().trim());
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
            catch (TokenizationException e) {
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
        }
        return token.getText();
    }

    private static TokenSource getLexer(String sql)
    {
        return new StatementLexer(new CaseInsensitiveStream(new ANTLRStringStream(sql)));
    }
}
