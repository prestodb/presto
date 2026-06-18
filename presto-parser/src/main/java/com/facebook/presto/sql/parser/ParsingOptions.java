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

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class ParsingOptions
{
    private static final Consumer<ParsingWarning> NOOP_WARNING_CONSUMER = ignored -> {};

    public enum DecimalLiteralTreatment
    {
        AS_DOUBLE,
        AS_DECIMAL,
        REJECT
    }

    private final DecimalLiteralTreatment decimalLiteralTreatment;
    private final Consumer<ParsingWarning> warningConsumer;

    /**
     * @deprecated Use (@link #builder())
     */
    @Deprecated
    public ParsingOptions()
    {
        this(DecimalLiteralTreatment.REJECT);
    }

    /**
     * @deprecated Use (@link #builder())
     */
    @Deprecated
    public ParsingOptions(DecimalLiteralTreatment decimalLiteralTreatment)
    {
        this(decimalLiteralTreatment, NOOP_WARNING_CONSUMER);
    }

    private ParsingOptions(DecimalLiteralTreatment decimalLiteralTreatment, Consumer<ParsingWarning> warningConsumer)
    {
        this.decimalLiteralTreatment = requireNonNull(decimalLiteralTreatment, "decimalLiteralTreatment is null");
        this.warningConsumer = requireNonNull(warningConsumer, "warningConsumer is null");
    }

    public DecimalLiteralTreatment getDecimalLiteralTreatment()
    {
        return decimalLiteralTreatment;
    }

    public Consumer<ParsingWarning> getWarningConsumer()
    {
        return warningConsumer;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private DecimalLiteralTreatment decimalLiteralTreatment = DecimalLiteralTreatment.REJECT;
        private Consumer<ParsingWarning> warningConsumer = NOOP_WARNING_CONSUMER;

        private Builder() {}

        public Builder setDecimalLiteralTreatment(DecimalLiteralTreatment decimalLiteralTreatment)
        {
            this.decimalLiteralTreatment = requireNonNull(decimalLiteralTreatment, "decimalLiteralTreatment is null");
            return this;
        }

        public Builder setWarningConsumer(Consumer<ParsingWarning> warningConsumer)
        {
            this.warningConsumer = requireNonNull(warningConsumer, "warningConsumer is null");
            return this;
        }

        public ParsingOptions build()
        {
            return new ParsingOptions(decimalLiteralTreatment, warningConsumer);
        }
    }
}
