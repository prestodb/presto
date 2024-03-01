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

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

@ThreadSafe
public final class RefreshableSqlBaseParserInitializer
        implements BiConsumer<SqlBaseLexer, SqlBaseParser>
{
    private final AtomicReference<SqlBaseParserAndLexerATNCaches> caches = new AtomicReference<>();

    public RefreshableSqlBaseParserInitializer()
    {
        refresh();
    }

    public void refresh()
    {
        caches.set(new SqlBaseParserAndLexerATNCaches());
    }

    @Override
    public void accept(SqlBaseLexer lexer, SqlBaseParser parser)
    {
        SqlBaseParserAndLexerATNCaches caches = this.caches.get();
        caches.lexer.configureLexer(lexer);
        caches.parser.configureParser(parser);
    }

    private static final class SqlBaseParserAndLexerATNCaches
    {
        public final AntlrATNCacheFields lexer = new AntlrATNCacheFields(SqlBaseLexer._ATN);
        public final AntlrATNCacheFields parser = new AntlrATNCacheFields(SqlBaseParser._ATN);
    }
}
