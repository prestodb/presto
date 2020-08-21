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
package com.facebook.presto.sql;

import com.facebook.presto.Session;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.parser.ParsingOptions;

import static com.facebook.presto.SystemSessionProperties.isParseDecimalLiteralsAsDouble;
import static com.facebook.presto.spi.StandardWarningCode.PARSER_WARNING;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;

public class ParsingUtil
{
    private ParsingUtil() {}

    public static ParsingOptions createParsingOptions(Session session)
    {
        return createParsingOptions(session, WarningCollector.NOOP);
    }

    public static ParsingOptions createParsingOptions(Session session, WarningCollector warningCollector)
    {
        return ParsingOptions.builder()
                .setDecimalLiteralTreatment(isParseDecimalLiteralsAsDouble(session) ? AS_DOUBLE : AS_DECIMAL)
                .setWarningConsumer(warning -> warningCollector.add(new PrestoWarning(PARSER_WARNING, warning.getMessage())))
                .build();
    }
}
