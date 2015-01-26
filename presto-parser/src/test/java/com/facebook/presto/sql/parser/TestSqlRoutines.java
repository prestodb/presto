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

import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AssignmentStatement;
import com.facebook.presto.sql.tree.CaseStatement;
import com.facebook.presto.sql.tree.CaseStatementWhenClause;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CompoundStatement;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateProcedure;
import com.facebook.presto.sql.tree.ElseClause;
import com.facebook.presto.sql.tree.ElseIfClause;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IfStatement;
import com.facebook.presto.sql.tree.IterateStatement;
import com.facebook.presto.sql.tree.LeaveStatement;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.LoopStatement;
import com.facebook.presto.sql.tree.ParameterDeclaration;
import com.facebook.presto.sql.tree.ParameterDeclaration.Mode;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.RepeatStatement;
import com.facebook.presto.sql.tree.ReturnClause;
import com.facebook.presto.sql.tree.ReturnStatement;
import com.facebook.presto.sql.tree.RoutineCharacteristic;
import com.facebook.presto.sql.tree.RoutineCharacteristics;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.VariableDeclaration;
import com.facebook.presto.sql.tree.WhileStatement;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.equal;
import static com.facebook.presto.sql.QueryUtil.functionCall;
import static com.facebook.presto.sql.QueryUtil.nameReference;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.table;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.testing.TreeAssertions.assertFormattedSql;
import static com.facebook.presto.sql.tree.DeterministicCharacteristic.DETERMINISTIC;
import static com.facebook.presto.sql.tree.DeterministicCharacteristic.NOT_DETERMINISTIC;
import static java.lang.String.format;
import static org.testng.Assert.fail;

public class TestSqlRoutines
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testSimpleFunction()
            throws Exception
    {
        assertStatement("" +
                        "CREATE FUNCTION hello(s CHAR)\n" +
                        "RETURNS CHAR DETERMINISTIC\n" +
                        "RETURN CONCAT('Hello, ', s, '!')",
                createFunction(
                        "hello",
                        ImmutableList.of(parameter("s", "CHAR")),
                        returns("CHAR"),
                        characteristics(DETERMINISTIC),
                        new ReturnStatement(
                                functionCall("CONCAT", literal("Hello, "), nameReference("s"), literal("!")))));
    }

    @Test
    public void testEmptyFunction()
            throws Exception
    {
        assertStatement("" +
                        "CREATE FUNCTION answer()\n" +
                        "RETURNS BIGINT\n" +
                        "RETURN 42",
                createFunction(
                        "answer",
                        ImmutableList.of(),
                        returns("BIGINT"),
                        characteristics(),
                        new ReturnStatement(literal(42))));
    }

    @Test
    public void testFibFunction()
            throws Exception
    {
        assertStatement("CREATE FUNCTION fib(n bigint)\n" +
                        "RETURNS bigint\n" +
                        "BEGIN\n" +
                        "  DECLARE a bigint DEFAULT 1;\n" +
                        "  DECLARE b bigint DEFAULT 1;\n" +
                        "  DECLARE c bigint;\n" +
                        "  IF n <= 2 THEN\n" +
                        "    RETURN 1;\n" +
                        "  END IF;\n" +
                        "  WHILE n > 2 DO\n" +
                        "    SET n = n - 1;\n" +
                        "    SET c = a + b;\n" +
                        "    SET a = b;\n" +
                        "    SET b = c;\n" +
                        "  END WHILE;\n" +
                        "  RETURN c;\n" +
                        "END",
                createFunction(
                        "fib",
                        ImmutableList.of(parameter("n", "bigint")),
                        returns("bigint"),
                        characteristics(),
                        beginEnd(ImmutableList.of(
                                        declare("a", "bigint", literal(1)),
                                        declare("b", "bigint", literal(1)),
                                        declare("c", "bigint")),
                                new IfStatement(
                                        lte("n", literal(2)),
                                        ImmutableList.of(new ReturnStatement(literal(1))),
                                        ImmutableList.of(),
                                        Optional.empty()),
                                new WhileStatement(
                                        Optional.empty(),
                                        gt("n", literal(2)),
                                        ImmutableList.of(
                                                assign("n", minus(nameReference("n"), literal(1))),
                                                assign("c", plus(nameReference("a"), nameReference("b"))),
                                                assign("a", nameReference("b")),
                                                assign("b", nameReference("c")))),
                                new ReturnStatement(nameReference("c")))));
    }

    @Test
    public void testFunctionWithIfElseIf()
            throws Exception
    {
        assertStatement("CREATE FUNCTION CustomerLevel(p_creditLimit double)\n" +
                        "RETURNS VARCHAR\n" +
                        "DETERMINISTIC\n" +
                        "BEGIN\n" +
                        "  DECLARE lvl varchar;\n" +
                        "  IF p_creditLimit > 50000 THEN\n" +
                        "    SET lvl = 'PLATINUM';\n" +
                        "  ELSEIF (p_creditLimit <= 50000 AND p_creditLimit >= 10000) THEN\n" +
                        "    SET lvl = 'GOLD';\n" +
                        "  ELSEIF p_creditLimit < 10000 THEN\n" +
                        "    SET lvl = 'SILVER';\n" +
                        "  END IF;\n" +
                        "  RETURN (lvl);\n" +
                        "END",
                createFunction(
                        "CustomerLevel",
                        ImmutableList.of(parameter("p_creditLimit", "double")),
                        returns("VARCHAR"),
                        characteristics(DETERMINISTIC),
                        beginEnd(ImmutableList.of(declare("lvl", "varchar")),
                                new IfStatement(
                                        gt("p_creditLimit", literal(50000)),
                                        ImmutableList.of(assign("lvl", literal("PLATINUM"))),
                                        ImmutableList.of(
                                                elseIf(new LogicalBinaryExpression(
                                                                LogicalBinaryExpression.Type.AND,
                                                                lte("p_creditLimit", literal(50000)),
                                                                gte("p_creditLimit", literal(10000))),
                                                        assign("lvl", literal("GOLD"))),
                                                elseIf(lt("p_creditLimit", literal(10000)),
                                                        assign("lvl", literal("SILVER")))),
                                        Optional.empty()),
                                new ReturnStatement(nameReference("lvl")))
                ));
    }

    @Test
    public void testSimpleProcedure()
            throws Exception
    {
        assertStatement("" +
                        "CREATE PROCEDURE simpleproc (OUT param1 INT)\n" +
                        "BEGIN\n" +
                        "  SELECT COUNT(*) INTO param1 FROM t;" +
                        "END",
                createProcedure(
                        "simpleproc",
                        ImmutableList.of(parameter(Mode.OUT, "param1", "INT")),
                        characteristics(),
                        beginEnd(ImmutableList.of(),
                                simpleQuery(
                                        simpleSelect(functionCall("count"), "param1"),
                                        table(new QualifiedName("t"))))));
    }

    @Test
    public void testProcedureWithCase()
            throws Exception
    {
        assertStatement("" +
                        "CREATE PROCEDURE GetCustomerShipping(\n" +
                        " in  p_customerNumber int, \n" +
                        " out p_shipping       varchar)\n" +
                        "NOT DETERMINISTIC\n" +
                        "BEGIN\n" +
                        "  DECLARE customerCountry varchar;\n" +
                        "  SELECT country INTO customerCountry\n" +
                        "  FROM customers\n" +
                        "  WHERE customerNumber = p_customerNumber;\n" +
                        "  CASE customerCountry\n" +
                        "  WHEN  'USA' THEN\n" +
                        "    SET p_shipping = '2-day Shipping';\n" +
                        "  WHEN 'Canada' THEN\n" +
                        "    SET p_shipping = '3-day Shipping';\n" +
                        "  ELSE\n" +
                        "    SET p_shipping = '5-day Shipping';\n" +
                        "  END CASE;\n" +
                        "END",
                createProcedure(
                        "GetCustomerShipping",
                        ImmutableList.of(
                                parameter(Mode.IN, "p_customerNumber", "int"),
                                parameter(Mode.OUT, "p_shipping", "varchar")),
                        characteristics(NOT_DETERMINISTIC),
                        beginEnd(ImmutableList.of(declare("customerCountry", "varchar")),
                                simpleQuery(
                                        simpleSelect(nameReference("country"), "customerCountry"),
                                        table(new QualifiedName("customers")),
                                        equal(nameReference("customerNumber"), nameReference("p_customerNumber"))),
                                new CaseStatement(
                                        Optional.of(nameReference("customerCountry")),
                                        ImmutableList.of(
                                                when(literal("USA"), assign("p_shipping", literal("2-day Shipping"))),
                                                when(literal("Canada"), assign("p_shipping", literal("3-day Shipping")))
                                        ),
                                        Optional.of(elseClause(assign("p_shipping", literal("5-day Shipping"))))))));
    }

    @Test
    public void testProcedureWithRepeat()
            throws Exception
    {
        assertStatement("" +
                        "CREATE PROCEDURE RepeatProc(OUT str VARCHAR)\n" +
                        "DETERMINISTIC\n" +
                        "BEGIN\n" +
                        "  DECLARE x INT;\n" +
                        "  SET x = 1;\n" +
                        "  SET str = '';\n" +
                        "  REPEAT\n" +
                        "    SET str = CONCAT(str, x, ',');\n" +
                        "    SET x = x + 1; \n" +
                        "  UNTIL x  > 5\n" +
                        "  END REPEAT;\n" +
                        "END",
                createProcedure(
                        "RepeatProc",
                        ImmutableList.of(parameter(Mode.OUT, "str", "VARCHAR")),
                        characteristics(DETERMINISTIC),
                        beginEnd(ImmutableList.of(declare("x", "INT")),
                                assign("x", literal(1)),
                                assign("str", literal("")),
                                new RepeatStatement(
                                        Optional.empty(),
                                        ImmutableList.of(
                                                assign(
                                                        "str",
                                                        functionCall(
                                                                "CONCAT",
                                                                nameReference("str"),
                                                                nameReference("x"),
                                                                literal(","))),
                                                assign("x", plus(nameReference("x"), literal(1)))
                                        ),
                                        gt("x", literal(5))))));
    }

    @Test
    public void testProcedureWithLoop()
            throws Exception
    {
        assertStatement("" +
                        "CREATE PROCEDURE LoopProc(OUT str VARCHAR)\n" +
                        "BEGIN\n" +
                        "  DECLARE x INT;\n" +
                        "  SET x = 1;\n" +
                        "  SET str =  '';\n" +
                        "loop_label: LOOP\n" +
                        "    IF x > 10 THEN\n" +
                        "      LEAVE loop_label;\n" +
                        "    END IF;\n" +
                        "    SET x = x + 1;\n" +
                        "    IF x % 2 THEN\n" +
                        "      ITERATE loop_label;\n" +
                        "    ELSE\n" +
                        "      SET str = CONCAT(str, x, ',');\n" +
                        "    END IF;\n" +
                        "  END LOOP;\n" +
                        "END",
                createProcedure(
                        "LoopProc",
                        ImmutableList.of(parameter(Mode.OUT, "str", "VARCHAR")),
                        characteristics(),
                        beginEnd(ImmutableList.of(declare("x", "INT")),
                                assign("x", literal(1)),
                                assign("str", literal("")),
                                new LoopStatement(
                                        Optional.of("loop_label"),
                                        ImmutableList.of(
                                                new IfStatement(
                                                        gt("x", literal(10)),
                                                        ImmutableList.of(new LeaveStatement("loop_label")),
                                                        ImmutableList.of(),
                                                        Optional.empty()),
                                                assign("x", plus(nameReference("x"), literal(1))),
                                                new IfStatement(
                                                        new ArithmeticBinaryExpression(
                                                                ArithmeticBinaryExpression.Type.MODULUS,
                                                                nameReference("x"),
                                                                literal(2)
                                                        ),
                                                        ImmutableList.of(new IterateStatement("loop_label")),
                                                        ImmutableList.of(),
                                                        Optional.of(
                                                                elseClause(assign(
                                                                        "str",
                                                                        functionCall(
                                                                                "CONCAT",
                                                                                nameReference("str"),
                                                                                nameReference("x"),
                                                                                literal(",")))))))))));
    }

    private static CreateProcedure createProcedure(
            String name,
            List<ParameterDeclaration> parameters,
            RoutineCharacteristics routineCharacteristics,
            Statement statement)
    {
        return new CreateProcedure(new QualifiedName(name), parameters, routineCharacteristics, statement);
    }

    private static CreateFunction createFunction(
            String name,
            List<ParameterDeclaration> parameters,
            ReturnClause returnClause,
            RoutineCharacteristics routineCharacteristics,
            Statement statement)
    {
        return new CreateFunction(new QualifiedName(name), parameters, returnClause, routineCharacteristics, statement);
    }

    private static RoutineCharacteristics characteristics(RoutineCharacteristic... characteristics)
    {
        return new RoutineCharacteristics(ImmutableList.copyOf(characteristics));
    }

    private static ReturnClause returns(String type)
    {
        return new ReturnClause(type, Optional.empty());
    }

    private static VariableDeclaration declare(String name, String type)
    {
        return new VariableDeclaration(ImmutableList.of(name), type, Optional.empty());
    }

    private static VariableDeclaration declare(String name, String type, Expression defaultValue)
    {
        return new VariableDeclaration(ImmutableList.of(name), type, Optional.of(defaultValue));
    }

    private static ParameterDeclaration parameter(String name, String type)
    {
        return new ParameterDeclaration(
                Optional.empty(),
                Optional.of(name),
                type,
                Optional.empty());
    }

    private static ParameterDeclaration parameter(Mode mode, String name, String type)
    {
        return new ParameterDeclaration(
                Optional.of(mode),
                Optional.of(name),
                type,
                Optional.empty());
    }

    private static AssignmentStatement assign(String name, Expression value)
    {
        return new AssignmentStatement(ImmutableList.of(new QualifiedName(name)), value);
    }

    private static ArithmeticBinaryExpression plus(Expression left, Expression right)
    {
        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.ADD, left, right);
    }

    private static ArithmeticBinaryExpression minus(Expression left, Expression right)
    {
        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.SUBTRACT, left, right);
    }

    private static ComparisonExpression lt(String name, Expression expression)
    {
        return new ComparisonExpression(
                ComparisonExpression.Type.LESS_THAN,
                nameReference(name),
                expression);
    }

    private static ComparisonExpression lte(String name, Expression expression)
    {
        return new ComparisonExpression(
                ComparisonExpression.Type.LESS_THAN_OR_EQUAL,
                nameReference(name),
                expression);
    }

    private static ComparisonExpression gt(String name, Expression expression)
    {
        return new ComparisonExpression(
                ComparisonExpression.Type.GREATER_THAN,
                nameReference(name),
                expression);
    }

    private static ComparisonExpression gte(String name, Expression expression)
    {
        return new ComparisonExpression(
                ComparisonExpression.Type.GREATER_THAN_OR_EQUAL,
                nameReference(name),
                expression);
    }

    private static StringLiteral literal(String literal)
    {
        return new StringLiteral(literal);
    }

    private static LongLiteral literal(long literal)
    {
        return new LongLiteral(String.valueOf(literal));
    }

    private static CompoundStatement beginEnd(List<VariableDeclaration> varDecls, Statement... statements)
    {
        return new CompoundStatement(Optional.empty(), varDecls, ImmutableList.copyOf(statements));
    }

    private static CaseStatementWhenClause when(Expression expression, Statement... statements)
    {
        return new CaseStatementWhenClause(expression, ImmutableList.copyOf(statements));
    }

    private static ElseIfClause elseIf(Expression expression, Statement... statements)
    {
        return new ElseIfClause(expression, ImmutableList.copyOf(statements));
    }

    private static ElseClause elseClause(Statement... statements)
    {
        return new ElseClause(ImmutableList.copyOf(statements));
    }

    private static Select simpleSelect(Expression expression, String target)
    {
        return new Select(
                false,
                ImmutableList.of(new SingleColumn(expression)),
                ImmutableList.of(new QualifiedName(target)));
    }

    private static void assertStatement(@Language("SQL") String query, Statement expected)
    {
        Statement parsed = SQL_PARSER.createStatement(query);
        if (!parsed.equals(expected)) {
            fail(format("expected\n\n%s\n\nto parse as\n\n%s\n\nbut was\n\n%s\n",
                    indent(query),
                    indent(formatSql(expected)),
                    indent(formatSql(parsed))));
        }
        assertFormattedSql(SQL_PARSER, parsed);
    }

    private static String indent(String value)
    {
        String indent = "    ";
        return indent + value.trim().replaceAll("\n", "\n" + indent);
    }
}
