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
package com.facebook.presto.parquet.batchreader;

import com.facebook.presto.parquet.batchreader.decoders.DefinitionLevelDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder;

public class DefinitionLevelValuesDecoderContext
{
    private final DefinitionLevelDecoder definitionLevelDecoder;
    private final ValuesDecoder valuesDecoder;
    private final int start;
    private final int end;

    public DefinitionLevelValuesDecoderContext(DefinitionLevelDecoder definitionLevelDecoder, ValuesDecoder valuesDecoder, int start, int end)
    {
        this.definitionLevelDecoder = definitionLevelDecoder;
        this.valuesDecoder = valuesDecoder;
        this.start = start;
        this.end = end;
    }

    public DefinitionLevelDecoder getDefinitionLevelDecoder()
    {
        return definitionLevelDecoder;
    }

    public ValuesDecoder getValuesDecoder()
    {
        return valuesDecoder;
    }

    public int getStart()
    {
        return start;
    }

    public int getEnd()
    {
        return end;
    }
}
