package com.facebook.presto.sql.parser;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import static com.facebook.presto.sql.parser.TreePrinter.treeToString;

public class PrintQuery
{
    public static void main(String[] args)
            throws Exception
    {
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("query> ");
            System.out.flush();

            String line = stdin.readLine();
            if (line == null) {
                break;
            }

            try {
                CommonTree tree = SqlParser.parseStatementList(line);
                System.out.println(treeToString(tree));
            }
            catch (RecognitionException e) {
                // fix race condition with console
                Thread.sleep(1);
            }
            System.out.println();
        }
    }
}
