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

import static java.util.Objects.requireNonNull;

public class ParsingOptions
{
    public enum DecimalLiteralTreatment
    {
        AS_DOUBLE,
        AS_DECIMAL,
        REJECT
    }

    private final DecimalLiteralTreatment decimalLiteralTreatment;

    public ParsingOptions()
    {
        this(DecimalLiteralTreatment.REJECT);
    }

    public ParsingOptions(boolean parseDecimalLiteralsAsDouble)
    {
        this(parseDecimalLiteralsAsDouble ? DecimalLiteralTreatment.AS_DOUBLE : DecimalLiteralTreatment.AS_DECIMAL);
    }

    public ParsingOptions(DecimalLiteralTreatment decimalLiteralTreatment)
    {
        this.decimalLiteralTreatment = requireNonNull(decimalLiteralTreatment, "decimalLiteralTreatment is null");
    }

    public DecimalLiteralTreatment getDecimalLiteralTreatment()
    {
        return decimalLiteralTreatment;
    }
}
