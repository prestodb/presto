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
package com.facebook.presto.orc.metadata.statistics;

import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.metadata.OrcType;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class StatisticsBuilders
{
    private StatisticsBuilders() {}

    public static Supplier<StatisticsBuilder> createFlatMapKeyStatisticsBuilderSupplier(OrcType orcType, ColumnWriterOptions columnWriterOptions)
    {
        requireNonNull(orcType, "orcType is null");
        requireNonNull(columnWriterOptions, "columnWriterOptions is null");

        switch (orcType.getOrcTypeKind()) {
            case BYTE:
                // TODO: sdruzkin - byte should use IntegerStatisticsBuilder
                return CountStatisticsBuilder::new;
            case SHORT:
            case INT:
            case LONG:
                return IntegerStatisticsBuilder::new;
            case BINARY:
                return BinaryStatisticsBuilder::new;
            case VARCHAR:
            case STRING:
                int stringStatisticsLimit = columnWriterOptions.getStringStatisticsLimit();
                return () -> new StringStatisticsBuilder(stringStatisticsLimit);
            default:
                throw new IllegalArgumentException("Unsupported type: " + orcType);
        }
    }
}
