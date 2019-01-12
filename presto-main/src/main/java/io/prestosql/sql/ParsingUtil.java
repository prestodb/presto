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
package io.prestosql.sql;

import io.prestosql.Session;
import io.prestosql.sql.parser.ParsingOptions;

import static io.prestosql.SystemSessionProperties.isParseDecimalLiteralsAsDouble;
import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;

public class ParsingUtil
{
    public static ParsingOptions createParsingOptions(Session session)
    {
        return new ParsingOptions(isParseDecimalLiteralsAsDouble(session) ? AS_DOUBLE : AS_DECIMAL);
    }

    private ParsingUtil() {}
}
