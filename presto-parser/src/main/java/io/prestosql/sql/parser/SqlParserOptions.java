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

import com.google.common.collect.Iterables;

import java.util.EnumSet;

import static java.util.Objects.requireNonNull;

public class SqlParserOptions
{
    private final EnumSet<IdentifierSymbol> allowedIdentifierSymbols = EnumSet.noneOf(IdentifierSymbol.class);
    private boolean enhancedErrorHandlerEnabled = true;

    public SqlParserOptions()
    {
    }

    private SqlParserOptions(EnumSet<IdentifierSymbol> identifierSymbols, boolean enhancedErrorHandlerEnabled)
    {
        this.enhancedErrorHandlerEnabled = enhancedErrorHandlerEnabled;
        this.allowedIdentifierSymbols.addAll(identifierSymbols);
    }

    public SqlParserOptions allowIdentifierSymbol(Iterable<IdentifierSymbol> identifierSymbols)
    {
        Iterables.addAll(allowedIdentifierSymbols, identifierSymbols);
        return this;
    }

    public EnumSet<IdentifierSymbol> getAllowedIdentifierSymbols()
    {
        return EnumSet.copyOf(allowedIdentifierSymbols);
    }

    public SqlParserOptions allowIdentifierSymbol(IdentifierSymbol... identifierSymbols)
    {
        for (IdentifierSymbol identifierSymbol : identifierSymbols) {
            allowedIdentifierSymbols.add(requireNonNull(identifierSymbol, "identifierSymbol is null"));
        }
        return this;
    }

    public SqlParserOptions useEnhancedErrorHandler(boolean enable)
    {
        enhancedErrorHandlerEnabled = enable;
        return this;
    }

    public boolean isEnhancedErrorHandlerEnabled()
    {
        return enhancedErrorHandlerEnabled;
    }

    public static SqlParserOptions copyOf(SqlParserOptions other)
    {
        return new SqlParserOptions(other.allowedIdentifierSymbols, other.enhancedErrorHandlerEnabled);
    }
}
