package com.facebook.presto.sql.parser;

import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.sql.parser.TreePrinter.treeToString;

public class TestSqlParser
{
    @Test
    public void testStatementBuilder()
            throws Exception
    {
        printStatement("select * from foo");

        printStatement("select * from foo a (x, y, z)");

        printStatement("" +
                "select * from foo a " +
                "join bar b using (bar_id) " +
                "left join zoo on (a.x = zoo.y) " +
                "cross join (d natural inner join e)");

        printStatement("select * from information_schema.tables");

        printStatement("show tables");
        printStatement("show tables from information_schema");

        for (int i = 1; i <= 22; i++) {
            if (i != 15) {
                printStatement(getTpchQuery(i));
            }
        }
    }

    private static void printStatement(String sql)
            throws RecognitionException
    {
        println(sql.trim());
        println("");

        CommonTree tree = SqlParser.parseStatement(sql);
        println(treeToString(tree));
        println("");

        Statement statement = SqlParser.createStatement(tree);
        println(statement.toString());
        println("");
    }

    private static void println(String s)
    {
        if (Boolean.getBoolean("printParse")) {
            System.out.println(s);
        }
    }

    private static String getTpchQuery(int q)
            throws IOException
    {
        return fixTpchQuery(readResource("tpch/queries/" + q + ".sql"));
    }

    private static String readResource(String name)
            throws IOException
    {
        return Resources.toString(Resources.getResource(name), Charsets.UTF_8);
    }

    private static String fixTpchQuery(String s)
    {
        s = s.replaceFirst("(?m);$", "");
        s = s.replaceAll("(?m)^:[xo]$", "");
        s = s.replaceAll("(?m)^:n -1$", "");
        s = s.replaceAll("(?m)^:n ([0-9]+)$", "LIMIT $1");
        s = s.replaceAll("([^']):([0-9]+)", "$1$2");
        return s;
    }
}
