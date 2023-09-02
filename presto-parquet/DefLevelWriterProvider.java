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
package io.trino.parquet.writer.repdef;

import com.google.common.collect.Iterables;
import org.apache.parquet.column.values.ValuesWriter;

import java.util.List;
import java.util.Optional;

public interface DefLevelWriterProvider
{
    DefinitionLevelWriter getDefinitionLevelWriter(Optional<DefinitionLevelWriter> nestedWriter, ValuesWriter encoder);

    interface DefinitionLevelWriter
    {
        ValuesCount writeDefinitionLevels(int positionsCount);

        ValuesCount writeDefinitionLevels();
    }

    record ValuesCount(int totalValuesCount, int maxDefinitionLevelValuesCount)
    {
    }

    static DefinitionLevelWriter getRootDefinitionLevelWriter(List<DefLevelWriterProvider> defLevelWriterProviders, ValuesWriter encoder)
    {
        // Constructs hierarchy of DefinitionLevelWriter from leaf to root
        DefinitionLevelWriter rootDefinitionLevelWriter = Iterables.getLast(defLevelWriterProviders)
                .getDefinitionLevelWriter(Optional.empty(), encoder);
        for (int nestedLevel = defLevelWriterProviders.size() - 2; nestedLevel >= 0; nestedLevel--) {
            DefinitionLevelWriter nestedWriter = rootDefinitionLevelWriter;
            rootDefinitionLevelWriter = defLevelWriterProviders.get(nestedLevel)
                    .getDefinitionLevelWriter(Optional.of(nestedWriter), encoder);
        }
        return rootDefinitionLevelWriter;
    }
}
