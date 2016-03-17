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
package com.facebook.presto.type;

import com.facebook.presto.sql.parser.CaseInsensitiveStream;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.type.TypeCalculationParser.ArithmeticBinaryContext;
import com.facebook.presto.type.TypeCalculationParser.ArithmeticUnaryContext;
import com.facebook.presto.type.TypeCalculationParser.BinaryFunctionContext;
import com.facebook.presto.type.TypeCalculationParser.IdentifierContext;
import com.facebook.presto.type.TypeCalculationParser.NullLiteralContext;
import com.facebook.presto.type.TypeCalculationParser.NumericLiteralContext;
import com.facebook.presto.type.TypeCalculationParser.ParenthesizedExpressionContext;
import com.facebook.presto.type.TypeCalculationParser.TypeCalculationContext;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;

import static com.facebook.presto.type.TypeCalculationParser.ASTERISK;
import static com.facebook.presto.type.TypeCalculationParser.MAX;
import static com.facebook.presto.type.TypeCalculationParser.MIN;
import static com.facebook.presto.type.TypeCalculationParser.MINUS;
import static com.facebook.presto.type.TypeCalculationParser.PLUS;
import static com.facebook.presto.type.TypeCalculationParser.SLASH;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class TypeCalculation
{
    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };

    private TypeCalculation() {}

    public static OptionalLong calculateLiteralValue(String calculation, Map<String, OptionalLong> inputs)
    {
        return calculateLiteralValue(calculation, inputs, true);
    }

    public static OptionalLong calculateLiteralValue(
            String calculation,
            Map<String, OptionalLong> inputs,
            boolean allowComplexExpression)
    {
        try {
            ParserRuleContext tree = parseTypeCalculation(calculation);
            if (!allowComplexExpression && !isSimpleExpression(tree)) {
                throw new IllegalArgumentException(format("Complex expressions not allowed, but got [%s]", calculation));
            }
            return new CalculateTypeVisitor(inputs).visit(tree);
        }
        catch (StackOverflowError e) {
            throw new ParsingException("Type calculation is too large (stack overflow while parsing)");
        }
    }

    private static ParserRuleContext parseTypeCalculation(String calculation)
    {
        TypeCalculationLexer lexer = new TypeCalculationLexer(new CaseInsensitiveStream(new ANTLRInputStream(calculation)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        TypeCalculationParser parser = new TypeCalculationParser(tokenStream);

        lexer.removeErrorListeners();
        lexer.addErrorListener(ERROR_LISTENER);

        parser.removeErrorListeners();
        parser.addErrorListener(ERROR_LISTENER);

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parser.typeCalculation();
        }
        catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.reset(); // rewind input stream
            parser.reset();

            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parser.typeCalculation();
        }
        return tree;
    }

    private static boolean isSimpleExpression(ParseTree tree)
    {
        return new IsSimpleExpressionVisitor().visit(tree);
    }

    private static class IsSimpleExpressionVisitor
            extends TypeCalculationBaseVisitor<Boolean>
    {
        @Override
        public Boolean visitArithmeticBinary(ArithmeticBinaryContext ctx)
        {
            return false;
        }

        @Override
        public Boolean visitArithmeticUnary(ArithmeticUnaryContext ctx)
        {
            return false;
        }

        @Override
        protected Boolean defaultResult()
        {
            return true;
        }

        @Override
        protected Boolean aggregateResult(Boolean aggregate, Boolean nextResult)
        {
            return aggregate && nextResult;
        }
    }

    private static class CalculateTypeVisitor
            extends TypeCalculationBaseVisitor<OptionalLong>
    {
        private final Map<String, OptionalLong> inputs;

        public CalculateTypeVisitor(Map<String, OptionalLong> inputs)
        {
            this.inputs = requireNonNull(inputs);
        }

        @Override
        public OptionalLong visitTypeCalculation(TypeCalculationContext ctx)
        {
            return visit(ctx.expression());
        }

        @Override
        public OptionalLong visitArithmeticBinary(ArithmeticBinaryContext ctx)
        {
            OptionalLong left = visit(ctx.left);
            OptionalLong right = visit(ctx.right);
            if (!left.isPresent() || !right.isPresent()) {
                return OptionalLong.empty();
            }
            switch (ctx.operator.getType()) {
                case PLUS:
                    return OptionalLong.of(left.getAsLong() + right.getAsLong());
                case MINUS:
                    return OptionalLong.of(left.getAsLong() - right.getAsLong());
                case ASTERISK:
                    return OptionalLong.of(left.getAsLong() * right.getAsLong());
                case SLASH:
                    return OptionalLong.of(left.getAsLong() / right.getAsLong());
                default:
                    throw new IllegalStateException("Unsupported binary operator " + ctx.operator.getText());
            }
        }

        @Override
        public OptionalLong visitArithmeticUnary(ArithmeticUnaryContext ctx)
        {
            OptionalLong value = visit(ctx.expression());
            if (!value.isPresent()) {
                return OptionalLong.empty();
            }
            switch (ctx.operator.getType()) {
                case PLUS:
                    return value;
                case MINUS:
                    return OptionalLong.of(-value.getAsLong());
                default:
                    throw new IllegalStateException("Unsupported unary operator " + ctx.operator.getText());
            }
        }

        @Override
        public OptionalLong visitBinaryFunction(BinaryFunctionContext ctx)
        {
            OptionalLong left = visit(ctx.left);
            OptionalLong right = visit(ctx.right);
            if (!left.isPresent() || !right.isPresent()) {
                return OptionalLong.empty();
            }
            switch(ctx.binaryFunctionName().name.getType()) {
                case MIN:
                    return OptionalLong.of(Math.min(left.getAsLong(), right.getAsLong()));
                case MAX:
                    return OptionalLong.of(Math.max(left.getAsLong(), right.getAsLong()));
                default:
                    throw new IllegalArgumentException("Unsupported binary function " + ctx.binaryFunctionName().getText());
            }
        }

        @Override
        public OptionalLong visitNumericLiteral(NumericLiteralContext ctx)
        {
            return OptionalLong.of(Long.parseLong(ctx.INTEGER_VALUE().getText()));
        }

        @Override
        public OptionalLong visitNullLiteral(NullLiteralContext ctx)
        {
            return OptionalLong.empty();
        }

        @Override
        public OptionalLong visitIdentifier(IdentifierContext ctx)
        {
            String identifier = ctx.getText();
            OptionalLong value = inputs.get(identifier.toUpperCase(Locale.US));
            if (value == null) {
                return OptionalLong.empty();
            }
            return value;
        }

        @Override
        public OptionalLong visitParenthesizedExpression(ParenthesizedExpressionContext ctx)
        {
            return visit(ctx.expression());
        }
    }
}
