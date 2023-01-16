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

import com.facebook.presto.jsonpath.JsonPathBaseListener;
import com.facebook.presto.jsonpath.JsonPathLexer;
import com.facebook.presto.jsonpath.JsonPathParser;
import com.facebook.presto.sql.jsonpath.tree.PathNode;
import com.facebook.presto.sql.parser.ParsingException;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public final class PathParser
{
    private final BaseErrorListener errorListener;

    public PathParser(Location startLocation)
    {
        requireNonNull(startLocation, "startLocation is null");

        int pathStartLine = startLocation.line;
        int pathStartColumn = startLocation.column;
        this.errorListener = new BaseErrorListener()
        {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
            {
                // The line and charPositionInLine correspond to the character within the string literal with JSON path expression.
                // Line and offset in error returned to the user should be computed based on the beginning of the whole query text.
                // We re-position the exception relatively to the start of the path expression within the query.
                int lineInQuery = pathStartLine - 1 + line;
                int columnInQuery = line == 1 ? pathStartColumn + 1 + charPositionInLine : charPositionInLine + 1;
                throw new ParsingException(message, e, lineInQuery, columnInQuery);
            }
        };
    }

    public PathNode parseJsonPath(String path)
    {
        try {
            // according to the SQL specification, the path language is case-sensitive in both identifiers and key words
            JsonPathLexer lexer = new JsonPathLexer(CharStreams.fromString(path));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            JsonPathParser parser = new JsonPathParser(tokenStream);

            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames()), parser));

            lexer.removeErrorListeners();
            lexer.addErrorListener(errorListener);

            parser.removeErrorListeners();
            parser.addErrorListener(errorListener);

            ParserRuleContext tree;
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parser.path();
            }
            catch (ParseCancellationException ex) {
                // if we fail, parse with LL mode
                tokenStream.seek(0); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parser.path();
            }

            return new PathTreeBuilder().visit(tree);
        }
        catch (StackOverflowError e) {
            throw new ParsingException("stack overflow while parsing JSON path");
        }
    }

    private static class PostProcessor
            extends JsonPathBaseListener
    {
        private final List<String> ruleNames;
        private final JsonPathParser parser;

        public PostProcessor(List<String> ruleNames, JsonPathParser parser)
        {
            this.ruleNames = ruleNames;
            this.parser = parser;
        }

        @Override
        public void exitNonReserved(JsonPathParser.NonReservedContext context)
        {
            // only a terminal can be replaced during rule exit event handling. Make sure that the nonReserved item is a token
            if (!(context.getChild(0) instanceof TerminalNode)) {
                int rule = ((ParserRuleContext) context.getChild(0)).getRuleIndex();
                throw new AssertionError("nonReserved can only contain tokens. Found nested rule: " + ruleNames.get(rule));
            }

            // replace nonReserved keyword with IDENTIFIER token
            context.getParent().removeLastChild();

            Token token = (Token) context.getChild(0).getPayload();
            Token newToken = new CommonToken(
                    new Pair<>(token.getTokenSource(), token.getInputStream()),
                    JsonPathLexer.IDENTIFIER,
                    token.getChannel(),
                    token.getStartIndex(),
                    token.getStopIndex());

            context.getParent().addChild(parser.createTerminalNode(context.getParent(), newToken));
        }
    }

    public static class Location
    {
        private final int line;
        private final int column;

        public Location(int line, int column)
        {
            if (line < 1) {
                throw new IllegalArgumentException("line must be at least 1");
            }

            if (column < 0) {
                throw new IllegalArgumentException("column must be at least 0");
            }

            this.line = line;
            this.column = column;
        }

        public int getLine()
        {
            return line;
        }

        public int getColumn()
        {
            return column;
        }
    }
}
