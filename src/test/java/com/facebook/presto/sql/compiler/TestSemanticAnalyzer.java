package com.facebook.presto.sql.compiler;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.TreePrinter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.sql.parser.TreePrinter.treeToString;

// TODO: add assertions
public class TestSemanticAnalyzer
{
    private Metadata metadata;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        Map<String, TableMetadata> tables = ImmutableMap.<String, TableMetadata>builder()
                .put("T", new TableMetadata("T", ImmutableList.of(
                        new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "id"),
                        new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "value"),
                        new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "title"),
                        new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "description"))))
                .put("S", new TableMetadata("S", ImmutableList.of(
                        new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "s_id"),
                        new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "name"),
                        new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "value"))))
                .build();

        Map<String, FunctionInfo> functions = ImmutableMap.<String, FunctionInfo>builder()
                .put("COUNT", new FunctionInfo(true))
                .put("SUM", new FunctionInfo(true))
                .put("AVG", new FunctionInfo(true))
                .put("MAX", new FunctionInfo(true))
                .put("MIN", new FunctionInfo(true))
                .build();

        metadata = new Metadata(tables, functions);
    }

    @Test
    public void testSimplePredicate()
            throws Exception
    {
        process("SELECT id FROM T WHERE value = 'hello'");
    }

    @Test
    public void testSubSelect()
            throws Exception
    {
        process("SELECT id, value FROM (" +
                "   SELECT id, value FROM T" +
                ") S");
    }

    @Test
    public void testGlobalAggregationFromSubSelect()
            throws Exception
    {
        process("SELECT SUM(value) FROM (" +
                "   SELECT id, value FROM T" +
                ") S");
    }

    @Test
    public void testMultiLevelSubSelect()
            throws Exception
    {
        process("SELECT b FROM (" +
                "   SELECT b" +
                "   FROM (" +
                "       SELECT id b FROM T" +
                "   ) U" +
                ") V");
    }

    @Test
    public void testInSubquery()
            throws Exception
    {
        process("SELECT id FROM T WHERE value IN (SELECT value FROM T)");
    }

    @Test
    public void testInSubqueryWithAlias()
            throws Exception
    {
        process("SELECT id " +
                "FROM T U " +
                "WHERE value IN (" +
                "   SELECT value " +
                "   FROM T " +
                "   WHERE id = U.id" +
                ")");
    }

    @Test
    public void testGroupByMultipleFields()
            throws Exception
    {
        process("SELECT id, value, SUM(value) FROM T WHERE value = 'hello' GROUP BY id, value");
    }

    @Test
    public void testComplex()
            throws Exception
    {
        String query = "" +
                "SELECT id, SUM(x) total FROM (" +
                "      SELECT id, title, AVG(value) x, MIN(value) y " +
                "      FROM T " +
                "      WHERE (description LIKE '%hello%' AND id > 10) OR value < 5 " +
                "      GROUP BY id, title " +
                ") S " +
                "WHERE id = title AND y > 20 " +
                "GROUP BY id";

        process(query);
    }

    @Test
    public void testHistogram()
            throws Exception
    {
        // histogram query
        String query = "" +
                "SELECT c, COUNT(*) " +
                "FROM ( " +
                "     SELECT id, COUNT(*) c " +
                "     FROM T " +
                "     GROUP BY id " +
                ") S " +
                "GROUP BY c";

        process(query);
    }

    @Test
    public void testGlobalAggregationWithArithmeticExpression()
            throws Exception
    {
        process("SELECT SUM(id) + SUM(value) * SUM(value) / AVG(value) FROM T");
    }

    @Test
    public void testAmbiguousReference()
            throws Exception
    {
        process("SELECT value FROM T, S");
    }

    @Test
    public void testSubselectWithAggregation()
            throws Exception
    {
        process("SELECT total FROM (SELECT COUNT(*) total FROM T) S");
    }

    @Test
    public void testHaving()
            throws Exception
    {
        process("SELECT value, SUM(id) total FROM T GROUP BY value HAVING SUM(id) > 10");
    }

    @Test
    public void testAllRelationColumns()
            throws Exception
    {
        process("SELECT T.* FROM T");
    }

    @Test
    public void testAllRelationColumnsFromMultipleTables()
            throws Exception
    {
        process("SELECT S.*, T.* FROM S, T");
    }

    @Test
    public void testAllRelationColumnsFromMultipleTables2()
            throws Exception
    {
        process("SELECT T.* FROM S, T");
    }

    @Test
    public void testAllRelationColumnsFromSubSelect()
            throws Exception
    {
        process("SELECT S.* FROM (" +
                "   SELECT * FROM T" +
                ") S");
    }

    @Test
    public void testAllRelationColumnsFromSubSelectUnnamed()
            throws Exception
    {
        process("SELECT U.* FROM (" +
                "   SELECT id + id, value + value FROM T" +
                ") U");
    }

    @Test
    public void testAllColumns()
            throws Exception
    {
        process("SELECT * FROM T");
    }

    @Test
    public void testAllColumnsFromMultipleTables()
            throws Exception
    {
        process("SELECT * FROM S, T");
    }

    @Test
    public void testAllColumnsFromSubSelect()
            throws Exception
    {
        process("SELECT * FROM (" +
                "   SELECT * FROM T" +
                ") S");
    }

    @Test
    public void testAllColumnsFromSubSelectUnnamed()
            throws Exception
    {
        process("SELECT * FROM (" +
                "   SELECT id + id, value + value FROM T" +
                ") U");
    }


    private void process(String query)
            throws RecognitionException
    {
        System.out.println(header("SQL", 80));
        System.out.println(query.replaceAll("\\s+", " "));
        System.out.println();

        System.out.println(header("Antlr AST", 80));
        CommonTree tree = SqlParser.parseStatement(query);
        System.out.println(treeToString(tree));
        System.out.println();

        System.out.println(header("AST", 80));
        Statement statement = SqlParser.createStatement(tree);

        try {
            SemanticAnalyzer analyzer = new SemanticAnalyzer(metadata);
            AnalysisResult result = analyzer.analyze(statement);

            new TreePrinter(result.getResolvedNames(), result.getTypes(), System.out)
                    .print(statement);

            System.out.println();
        }
        catch (SemanticException e) {
            System.out.println(e.getMessage() + " at " + e.getNode());
        }
    }

    private String header(String title, int width)
    {
        int barWidth = (width - title.length() - 2) / 2;
        String bar = Strings.repeat("=", barWidth);
        return bar + " " + title + " " + bar;
    }

}

