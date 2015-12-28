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

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.type.TypeCalculationParser.ArithmeticBinaryContext;
import com.facebook.presto.type.TypeCalculationParser.ArithmeticUnaryContext;
import com.facebook.presto.type.TypeCalculationParser.IdentifierContext;
import com.facebook.presto.type.TypeCalculationParser.NullLiteralContext;
import com.facebook.presto.type.TypeCalculationParser.NumericLiteralContext;
import com.facebook.presto.type.TypeCalculationParser.ParenthesizedExpressionContext;
import com.facebook.presto.type.TypeCalculationParser.TypeCalculationContext;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;

import static com.facebook.presto.type.TypeCalculationParser.ASTERISK;
import static com.facebook.presto.type.TypeCalculationParser.MINUS;
import static com.facebook.presto.type.TypeCalculationParser.PLUS;
import static com.facebook.presto.type.TypeCalculationParser.SLASH;
import static java.lang.String.format;

public final class TypeCalculation
{
    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(@NotNull Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, @NotNull String message, RecognitionException e)
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
            boolean allowExpressionInCalculation)
    {
        try {
            ParserRuleContext tree = parseTypeCalculation(calculation);
            if (!allowExpressionInCalculation && !(new IsSingleNodeVisitor().visit(tree))) {
                throw new IllegalArgumentException(format("Expressions not allowed, but got [%s]", calculation));
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

    private static class IsSingleNodeVisitor
            extends TypeCalculationBaseVisitor<Boolean>
    {
        @Override
        public Boolean visitArithmeticBinary(@NotNull ArithmeticBinaryContext ctx)
        {
            return false;
        }

        @Override
        public Boolean visitArithmeticUnary(@NotNull ArithmeticUnaryContext ctx)
        {
            return false;
        }

        protected Boolean defaultResult()
        {
            return true;
        }

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
            this.inputs = inputs;
        }

        @Override
        public OptionalLong visitTypeCalculation(@NotNull TypeCalculationContext ctx)
        {
            return visit(ctx.expression());
        }

        @Override
        public OptionalLong visitArithmeticBinary(@NotNull ArithmeticBinaryContext ctx)
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
        public OptionalLong visitArithmeticUnary(@NotNull ArithmeticUnaryContext ctx)
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
        public OptionalLong visitNumericLiteral(@NotNull NumericLiteralContext ctx)
        {
            return OptionalLong.of(Long.parseLong(ctx.INTEGER_VALUE().getText()));
        }

        @Override
        public OptionalLong visitNullLiteral(@NotNull NullLiteralContext ctx)
        {
            return OptionalLong.empty();
        }

        @Override
        public OptionalLong visitIdentifier(@NotNull IdentifierContext ctx)
        {
            String identifier = ctx.getText();
            OptionalLong value = inputs.get(identifier.toUpperCase(Locale.US));
            if (value == null) {
                throw new IllegalArgumentException("No value for " + identifier);
            }
            return value;
        }

        @Override
        public OptionalLong visitParenthesizedExpression(@NotNull ParenthesizedExpressionContext ctx)
        {
            return visit(ctx.expression());
        }
    }

    private static class CaseInsensitiveStream
            implements CharStream
    {
        private final CharStream stream;

        public CaseInsensitiveStream(CharStream stream)
        {
            this.stream = stream;
        }

        @Override
        @NotNull
        public String getText(@NotNull Interval interval)
        {
            return stream.getText(interval);
        }

        @Override
        public void consume()
        {
            stream.consume();
        }

        @Override
        public int LA(int i)
        {
            int result = stream.LA(i);

            switch (result) {
                case 0:
                case CharStream.EOF:
                    return result;
                default:
                    return Character.toUpperCase(result);
            }
        }

        @Override
        public int mark()
        {
            return stream.mark();
        }

        @Override
        public void release(int marker)
        {
            stream.release(marker);
        }

        @Override
        public int index()
        {
            return stream.index();
        }

        @Override
        public void seek(int index)
        {
            stream.seek(index);
        }

        @Override
        public int size()
        {
            return stream.size();
        }

        @Override
        @NotNull
        public String getSourceName()
        {
            return stream.getSourceName();
        }
    }
}
