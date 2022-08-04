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
package com.facebook.presto.sql.jsonpath;

import com.facebook.presto.jsonpath.JsonPathBaseVisitor;
import com.facebook.presto.jsonpath.JsonPathParser;
import com.facebook.presto.sql.jsonpath.tree.AbsMethod;
import com.facebook.presto.sql.jsonpath.tree.ArithmeticBinary;
import com.facebook.presto.sql.jsonpath.tree.ArithmeticBinary.Operator;
import com.facebook.presto.sql.jsonpath.tree.ArithmeticUnary;
import com.facebook.presto.sql.jsonpath.tree.ArithmeticUnary.Sign;
import com.facebook.presto.sql.jsonpath.tree.ArrayAccessor;
import com.facebook.presto.sql.jsonpath.tree.CeilingMethod;
import com.facebook.presto.sql.jsonpath.tree.ComparisonPredicate;
import com.facebook.presto.sql.jsonpath.tree.ConjunctionPredicate;
import com.facebook.presto.sql.jsonpath.tree.ContextVariable;
import com.facebook.presto.sql.jsonpath.tree.DatetimeMethod;
import com.facebook.presto.sql.jsonpath.tree.DisjunctionPredicate;
import com.facebook.presto.sql.jsonpath.tree.DoubleMethod;
import com.facebook.presto.sql.jsonpath.tree.ExistsPredicate;
import com.facebook.presto.sql.jsonpath.tree.Filter;
import com.facebook.presto.sql.jsonpath.tree.FloorMethod;
import com.facebook.presto.sql.jsonpath.tree.IsUnknownPredicate;
import com.facebook.presto.sql.jsonpath.tree.JsonPath;
import com.facebook.presto.sql.jsonpath.tree.KeyValueMethod;
import com.facebook.presto.sql.jsonpath.tree.LastIndexVariable;
import com.facebook.presto.sql.jsonpath.tree.LikeRegexPredicate;
import com.facebook.presto.sql.jsonpath.tree.MemberAccessor;
import com.facebook.presto.sql.jsonpath.tree.NamedVariable;
import com.facebook.presto.sql.jsonpath.tree.NegationPredicate;
import com.facebook.presto.sql.jsonpath.tree.PathNode;
import com.facebook.presto.sql.jsonpath.tree.Predicate;
import com.facebook.presto.sql.jsonpath.tree.PredicateCurrentItemVariable;
import com.facebook.presto.sql.jsonpath.tree.SizeMethod;
import com.facebook.presto.sql.jsonpath.tree.SqlValueLiteral;
import com.facebook.presto.sql.jsonpath.tree.StartsWithPredicate;
import com.facebook.presto.sql.jsonpath.tree.TypeMethod;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Optional;

import static com.facebook.presto.sql.jsonpath.tree.JsonNullLiteral.JSON_NULL;

public class PathTreeBuilder
        extends JsonPathBaseVisitor<PathNode>
{
    @Override
    public PathNode visitPath(JsonPathParser.PathContext context)
    {
        boolean lax = context.pathMode().LAX() != null;
        PathNode path = visit(context.pathExpression());
        return new JsonPath(lax, path);
    }

    @Override
    public PathNode visitDecimalLiteral(JsonPathParser.DecimalLiteralContext context)
    {
        return new SqlValueLiteral(new DecimalLiteral(context.getText()));
    }

    @Override
    public PathNode visitDoubleLiteral(JsonPathParser.DoubleLiteralContext context)
    {
        return new SqlValueLiteral(new DoubleLiteral(context.getText()));
    }

    @Override
    public PathNode visitIntegerLiteral(JsonPathParser.IntegerLiteralContext context)
    {
        return new SqlValueLiteral(new LongLiteral(context.getText()));
    }

    @Override
    public PathNode visitStringLiteral(JsonPathParser.StringLiteralContext context)
    {
        return new SqlValueLiteral(new StringLiteral(unquote(context.STRING().getText())));
    }

    private static String unquote(String quoted)
    {
        return quoted.substring(1, quoted.length() - 1)
                .replace("\"\"", "\"");
    }

    @Override
    public PathNode visitNullLiteral(JsonPathParser.NullLiteralContext context)
    {
        return JSON_NULL;
    }

    @Override
    public PathNode visitBooleanLiteral(JsonPathParser.BooleanLiteralContext context)
    {
        return new SqlValueLiteral(new BooleanLiteral(context.getText()));
    }

    @Override
    public PathNode visitContextVariable(JsonPathParser.ContextVariableContext context)
    {
        return new ContextVariable();
    }

    @Override
    public PathNode visitNamedVariable(JsonPathParser.NamedVariableContext context)
    {
        return namedVariable(context.NAMED_VARIABLE());
    }

    private static NamedVariable namedVariable(TerminalNode namedVariable)
    {
        // drop leading `$`
        return new NamedVariable(namedVariable.getText().substring(1));
    }

    @Override
    public PathNode visitLastIndexVariable(JsonPathParser.LastIndexVariableContext context)
    {
        return new LastIndexVariable();
    }

    @Override
    public PathNode visitPredicateCurrentItemVariable(JsonPathParser.PredicateCurrentItemVariableContext context)
    {
        return new PredicateCurrentItemVariable();
    }

    @Override
    public PathNode visitParenthesizedPath(JsonPathParser.ParenthesizedPathContext context)
    {
        return visit(context.pathExpression());
    }

    @Override
    public PathNode visitMemberAccessor(JsonPathParser.MemberAccessorContext context)
    {
        PathNode base = visit(context.accessorExpression());
        Optional<String> key = Optional.empty();
        if (context.stringLiteral() != null) {
            key = Optional.of(unquote(context.stringLiteral().getText()));
        }
        else if (context.identifier() != null) {
            key = Optional.of(context.identifier().getText());
        }
        return new MemberAccessor(base, key);
    }

    @Override
    public PathNode visitWildcardMemberAccessor(JsonPathParser.WildcardMemberAccessorContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new MemberAccessor(base, Optional.empty());
    }

    @Override
    public PathNode visitArrayAccessor(JsonPathParser.ArrayAccessorContext context)
    {
        PathNode base = visit(context.accessorExpression());
        ImmutableList.Builder<ArrayAccessor.Subscript> subscripts = ImmutableList.builder();
        for (JsonPathParser.SubscriptContext subscript : context.subscript()) {
            if (subscript.singleton != null) {
                subscripts.add(new ArrayAccessor.Subscript(visit(subscript.singleton)));
            }
            else {
                subscripts.add(new ArrayAccessor.Subscript(visit(subscript.from), visit(subscript.to)));
            }
        }
        return new ArrayAccessor(base, subscripts.build());
    }

    @Override
    public PathNode visitWildcardArrayAccessor(JsonPathParser.WildcardArrayAccessorContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new ArrayAccessor(base, ImmutableList.of());
    }

    @Override
    public PathNode visitFilter(JsonPathParser.FilterContext context)
    {
        PathNode base = visit(context.accessorExpression());
        Predicate predicate = (Predicate) visit(context.predicate());
        return new Filter(base, predicate);
    }

    @Override
    public PathNode visitTypeMethod(JsonPathParser.TypeMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new TypeMethod(base);
    }

    @Override
    public PathNode visitSizeMethod(JsonPathParser.SizeMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new SizeMethod(base);
    }

    @Override
    public PathNode visitDoubleMethod(JsonPathParser.DoubleMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new DoubleMethod(base);
    }

    @Override
    public PathNode visitCeilingMethod(JsonPathParser.CeilingMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new CeilingMethod(base);
    }

    @Override
    public PathNode visitFloorMethod(JsonPathParser.FloorMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new FloorMethod(base);
    }

    @Override
    public PathNode visitAbsMethod(JsonPathParser.AbsMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new AbsMethod(base);
    }

    @Override
    public PathNode visitDatetimeMethod(JsonPathParser.DatetimeMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        Optional<String> format = Optional.empty();
        if (context.stringLiteral() != null) {
            format = Optional.of(unquote(context.stringLiteral().getText()));
        }
        return new DatetimeMethod(base, format);
    }

    @Override
    public PathNode visitKeyValueMethod(JsonPathParser.KeyValueMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new KeyValueMethod(base);
    }

    @Override
    public PathNode visitSignedUnary(JsonPathParser.SignedUnaryContext context)
    {
        PathNode base = visit(context.pathExpression());
        return new ArithmeticUnary(getSign(context.sign.getText()), base);
    }

    private static Sign getSign(String operator)
    {
        switch (operator) {
            case "+":
                return Sign.PLUS;
            case "-":
                return Sign.MINUS;
            default:
                throw new UnsupportedOperationException("unexpected unary operator: " + operator);
        }
    }

    @Override
    public PathNode visitBinary(JsonPathParser.BinaryContext context)
    {
        PathNode left = visit(context.left);
        PathNode right = visit(context.right);
        return new ArithmeticBinary(getOperator(context.operator.getText()), left, right);
    }

    private static Operator getOperator(String operator)
    {
        switch (operator) {
            case "+":
                return Operator.ADD;
            case "-":
                return Operator.SUBTRACT;
            case "*":
                return Operator.MULTIPLY;
            case "/":
                return Operator.DIVIDE;
            case "%":
                return Operator.MODULUS;
            default:
                throw new UnsupportedOperationException("unexpected binary operator: " + operator);
        }
    }

    // predicate

    @Override
    public PathNode visitComparisonPredicate(JsonPathParser.ComparisonPredicateContext context)
    {
        PathNode left = visit(context.left);
        PathNode right = visit(context.right);
        return new ComparisonPredicate(getComparisonOperator(context.comparisonOperator().getText()), left, right);
    }

    private static ComparisonPredicate.Operator getComparisonOperator(String operator)
    {
        switch (operator) {
            case "==":
                return ComparisonPredicate.Operator.EQUAL;
            case "<>":
            case "!=":
                return ComparisonPredicate.Operator.NOT_EQUAL;
            case "<":
                return ComparisonPredicate.Operator.LESS_THAN;
            case ">":
                return ComparisonPredicate.Operator.GREATER_THAN;
            case "<=":
                return ComparisonPredicate.Operator.LESS_THAN_OR_EQUAL;
            case ">=":
                return ComparisonPredicate.Operator.GREATER_THAN_OR_EQUAL;
            default:
                throw new UnsupportedOperationException("unexpected comparison operator: " + operator);
        }
    }

    @Override
    public PathNode visitConjunctionPredicate(JsonPathParser.ConjunctionPredicateContext context)
    {
        Predicate left = (Predicate) visit(context.left);
        Predicate right = (Predicate) visit(context.right);
        return new ConjunctionPredicate(left, right);
    }

    @Override
    public PathNode visitDisjunctionPredicate(JsonPathParser.DisjunctionPredicateContext context)
    {
        Predicate left = (Predicate) visit(context.left);
        Predicate right = (Predicate) visit(context.right);
        return new DisjunctionPredicate(left, right);
    }

    @Override
    public PathNode visitExistsPredicate(JsonPathParser.ExistsPredicateContext context)
    {
        PathNode path = visit(context.pathExpression());
        return new ExistsPredicate(path);
    }

    @Override
    public PathNode visitIsUnknownPredicate(JsonPathParser.IsUnknownPredicateContext context)
    {
        Predicate predicate = (Predicate) visit(context.predicate());
        return new IsUnknownPredicate(predicate);
    }

    @Override
    public PathNode visitLikeRegexPredicate(JsonPathParser.LikeRegexPredicateContext context)
    {
        PathNode path = visit(context.base);
        String pattern = unquote(context.pattern.getText());
        Optional<String> flag = Optional.empty();
        if (context.flag != null) {
            flag = Optional.of(unquote(context.flag.getText()));
        }
        return new LikeRegexPredicate(path, pattern, flag);
    }

    @Override
    public PathNode visitNegationPredicate(JsonPathParser.NegationPredicateContext context)
    {
        Predicate predicate = (Predicate) visit(context.delimitedPredicate());
        return new NegationPredicate(predicate);
    }

    @Override
    public PathNode visitParenthesizedPredicate(JsonPathParser.ParenthesizedPredicateContext context)
    {
        return visit(context.predicate());
    }

    @Override
    public PathNode visitStartsWithPredicate(JsonPathParser.StartsWithPredicateContext context)
    {
        PathNode whole = visit(context.whole);
        PathNode initial;
        if (context.string != null) {
            initial = visit(context.string);
        }
        else {
            initial = namedVariable(context.NAMED_VARIABLE());
        }
        return new StartsWithPredicate(whole, initial);
    }

    @Override
    protected PathNode aggregateResult(PathNode aggregate, PathNode nextResult)
    {
        // do not skip over unrecognized nodes
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        if (aggregate == null) {
            return nextResult;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }
}
