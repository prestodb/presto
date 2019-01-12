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
package io.prestosql.sql.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StatementSplitter
{
    private final List<Statement> completeStatements;
    private final String partialStatement;

    public StatementSplitter(String sql)
    {
        this(sql, ImmutableSet.of(";"));
    }

    public StatementSplitter(String sql, Set<String> delimiters)
    {
        TokenSource tokens = getLexer(sql, delimiters);
        ImmutableList.Builder<Statement> list = ImmutableList.builder();
        StringBuilder sb = new StringBuilder();
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                break;
            }
            if (token.getType() == SqlBaseParser.DELIMITER) {
                String statement = sb.toString().trim();
                if (!statement.isEmpty()) {
                    list.add(new Statement(statement, token.getText()));
                }
                sb = new StringBuilder();
            }
            else {
                sb.append(token.getText());
            }
        }
        this.completeStatements = list.build();
        this.partialStatement = sb.toString().trim();
    }

    public List<Statement> getCompleteStatements()
    {
        return completeStatements;
    }

    public String getPartialStatement()
    {
        return partialStatement;
    }

    public static String squeezeStatement(String sql)
    {
        TokenSource tokens = getLexer(sql, ImmutableSet.of());
        StringBuilder sb = new StringBuilder();
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                break;
            }
            if (token.getType() == SqlBaseLexer.WS) {
                sb.append(' ');
            }
            else {
                sb.append(token.getText());
            }
        }
        return sb.toString().trim();
    }

    public static boolean isEmptyStatement(String sql)
    {
        TokenSource tokens = getLexer(sql, ImmutableSet.of());
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                return true;
            }
            if (token.getChannel() != Token.HIDDEN_CHANNEL) {
                return false;
            }
        }
    }

    private static TokenSource getLexer(String sql, Set<String> terminators)
    {
        requireNonNull(sql, "sql is null");
        CharStream stream = new CaseInsensitiveStream(new ANTLRInputStream(sql));
        return new DelimiterLexer(stream, terminators);
    }

    public static class Statement
    {
        private final String statement;
        private final String terminator;

        public Statement(String statement, String terminator)
        {
            this.statement = requireNonNull(statement, "statement is null");
            this.terminator = requireNonNull(terminator, "terminator is null");
        }

        public String statement()
        {
            return statement;
        }

        public String terminator()
        {
            return terminator;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            Statement o = (Statement) obj;
            return Objects.equals(statement, o.statement) &&
                    Objects.equals(terminator, o.terminator);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(statement, terminator);
        }

        @Override
        public String toString()
        {
            return statement + terminator;
        }
    }
}
