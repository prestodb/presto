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

public class SqlParser
{
    public static Statement createStatement(String sql)
            throws RecognitionException
    {
        return createStatement(parseStatement(sql));
    }

    public static Expression createExpression(String expression)
            throws RecognitionException
    {
        return createExpression(parseExpression(expression));
    }

    @VisibleForTesting
    static Statement createStatement(CommonTree tree)
            throws RecognitionException
    {
        TreeNodeStream stream = new BufferedTreeNodeStream(tree);
        StatementBuilder builder = new StatementBuilder(stream);
        return builder.statement().value;
    }

    private static Expression createExpression(CommonTree tree)
            throws RecognitionException
    {
        TreeNodeStream stream = new BufferedTreeNodeStream(tree);
        StatementBuilder builder = new StatementBuilder(stream);
        return builder.expr().value;
    }

    @VisibleForTesting
    static CommonTree parseStatement(String sql)
            throws RecognitionException
    {
        return (CommonTree) getParser(sql).singleStatement().getTree();
    }

    private static CommonTree parseExpression(String expression)
            throws RecognitionException
    {
        return (CommonTree) getParser(expression).expr().getTree();
    }

    private static StatementParser getParser(String sql)
    {
        CharStream stream = new CaseInsensitiveStream(new ANTLRStringStream(sql));
        StatementLexer lexer = new StatementLexer(stream);
        TokenStream tokenStream = new CommonTokenStream(lexer);
        return new StatementParser(tokenStream);
    }
}
