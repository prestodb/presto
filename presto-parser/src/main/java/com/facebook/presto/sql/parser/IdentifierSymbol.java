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

import org.antlr.runtime.IntStream;
import org.antlr.runtime.RecognitionException;

import java.util.EnumSet;

public enum IdentifierSymbol
{
    COLON(':'),
    AT_SIGN('@');

    private final char symbol;

    IdentifierSymbol(char symbol)
    {
        this.symbol = symbol;
    }

    public char getSymbol()
    {
        return symbol;
    }

    public static void validateIdentifier(IntStream input, String name, EnumSet<IdentifierSymbol> allowedSymbols)
    {
        for (IdentifierSymbol identifierSymbol : EnumSet.complementOf(allowedSymbols)) {
            char symbol = identifierSymbol.getSymbol();
            if (name.indexOf(symbol) >= 0) {
                // rewind so that the error location is at the start of the identifier
                input.rewind();
                throw new ParsingException("identifiers must not contain '" + identifierSymbol.getSymbol() + "'", new RecognitionException(input));
            }
        }
    }
}
