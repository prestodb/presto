package com.facebook.presto.sql.parser;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.parser.StatementSplitter.squeezeStatement;
import static org.testng.Assert.assertEquals;

public class TestStatementSplitter
{
    @Test
    public void testSplitterIncomplete()
    {
        StatementSplitter splitter = new StatementSplitter(" select * FROM foo  ");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), "select * FROM foo");
    }

    @Test
    public void testSplitterEmptyInput()
    {
        StatementSplitter splitter = new StatementSplitter("");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), "");
    }

    @Test
    public void testSplitterEmptyStatements()
    {
        StatementSplitter splitter = new StatementSplitter(";;;");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), "");
    }

    @Test
    public void testSplitterSingle()
    {
        StatementSplitter splitter = new StatementSplitter("select * from foo;");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of("select * from foo"));
        assertEquals(splitter.getPartialStatement(), "");
    }

    @Test
    public void testSplitterMultiple()
    {
        StatementSplitter splitter = new StatementSplitter(" select * from  foo ; select * from t; select * from ");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of("select * from  foo", "select * from t"));
        assertEquals(splitter.getPartialStatement(), "select * from");
    }

    @Test
    public void testSplitterMultipleWithEmpty()
    {
        StatementSplitter splitter = new StatementSplitter("; select * from  foo ; select * from t;;;select * from ");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of("select * from  foo", "select * from t"));
        assertEquals(splitter.getPartialStatement(), "select * from");
    }

    @Test
    public void testSplitterErrorBeforeComplete()
    {
        StatementSplitter splitter = new StatementSplitter(" select * from z@ oops ; select ");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of("select * from z@ oops"));
        assertEquals(splitter.getPartialStatement(), "select");
    }

    @Test
    public void testSplitterErrorAfterComplete()
    {
        StatementSplitter splitter = new StatementSplitter("select * from foo; select z@ oops ");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of("select * from foo"));
        assertEquals(splitter.getPartialStatement(), "select z@ oops");
    }

    @Test
    public void testSplitterWithQuotedString()
    {
        String sql = "select 'foo bar' x from dual";
        StatementSplitter splitter = new StatementSplitter(sql);
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), sql);
    }

    @Test
    public void testSplitterWithIncompleteQuotedString()
    {
        String sql = "select 'foo', 'bar";
        StatementSplitter splitter = new StatementSplitter(sql);
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), sql);
    }

    @Test
    public void testSplitterWithEscapedSingleQuote()
    {
        String sql = "select 'hello''world' from dual";
        StatementSplitter splitter = new StatementSplitter(sql + ";");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of(sql));
        assertEquals(splitter.getPartialStatement(), "");
    }

    @Test
    public void testSplitterWithQuotedIdentifier()
    {
        String sql = "select \"0\"\"bar\" from dual";
        StatementSplitter splitter = new StatementSplitter(sql + ";");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of(sql));
        assertEquals(splitter.getPartialStatement(), "");
    }

    @Test
    public void testSplitterWithBackquote()
    {
        String sql = "select  ` f``o o ` from dual";
        StatementSplitter splitter = new StatementSplitter(sql);
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), sql);
    }

    @Test
    public void testSqueezeStatement()
    {
        String sql = "select   *  from\n foo\n  order by x ; ";
        assertEquals(squeezeStatement(sql), "select * from foo order by x ;");
    }

    @Test
    public void testSqueezeStatementWithIncompleteQuotedString()
    {
        String sql = "select   *  from\n foo\n  where x = 'oops";
        assertEquals(squeezeStatement(sql), "select * from foo where x = 'oops");
    }

    @Test
    public void testSqueezeStatementWithBackquote()
    {
        String sql = "select  `  f``o  o`` `   from dual";
        assertEquals(squeezeStatement(sql), "select `  f``o  o`` ` from dual");
    }

    @Test
    public void testSqueezeStatementError()
    {
        String sql = "select   *  from z@oops";
        assertEquals(squeezeStatement(sql), "select * from z@oops");
    }
}
