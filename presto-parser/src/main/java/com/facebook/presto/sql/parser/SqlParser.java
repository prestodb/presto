package com.facebook.presto.sql.parser;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.annotations.VisibleForTesting;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.BufferedTreeNodeStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.TreeNodeStream;

public final class SqlParser
{
    private SqlParser() {}

    public static Statement createStatement(String sql)
    {
        try {
            return createStatement(parseStatement(sql));
        }
        catch (StackOverflowError e) {
            throw new ParsingException("statement is too large (stack overflow while parsing)");
        }
    }

    public static Expression createExpression(String expression)
    {
        try {
            return createExpression(parseExpression(expression));
        }
        catch (StackOverflowError e) {
            throw new ParsingException("expression is too large (stack overflow while parsing)");
        }
    }

    @VisibleForTesting
    static Statement createStatement(CommonTree tree)
    {
        TreeNodeStream stream = new BufferedTreeNodeStream(tree);
        StatementBuilder builder = new StatementBuilder(stream);
        try {
            return builder.statement().value;
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    private static Expression createExpression(CommonTree tree)
    {
        TreeNodeStream stream = new BufferedTreeNodeStream(tree);
        StatementBuilder builder = new StatementBuilder(stream);
        try {
            return builder.singleExpression().value;
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    @VisibleForTesting
    static CommonTree parseStatement(String sql)
    {
        try {
            return (CommonTree) getParser(sql).singleStatement().getTree();
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    private static CommonTree parseExpression(String expression)
    {
        try {
            return (CommonTree) getParser(expression).singleExpression().getTree();
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    private static StatementParser getParser(String sql)
    {
        CharStream stream = new CaseInsensitiveStream(new ANTLRStringStream(sql));
        StatementLexer lexer = new StatementLexer(stream);
        TokenStream tokenStream = new CommonTokenStream(lexer);
        return new StatementParser(tokenStream);
    }
}
