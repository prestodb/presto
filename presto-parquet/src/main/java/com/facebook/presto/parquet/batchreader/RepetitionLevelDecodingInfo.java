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

import java.util.LinkedList;
import java.util.List;

public class RepetitionLevelDecodingInfo
{
    private final List<DefinitionLevelValuesDecoderInfo> definitionLevelValuesDecoderInfos = new LinkedList<>();
    private int[] repetitionLevels;

    public void add(DefinitionLevelValuesDecoderInfo definitionLevelValuesDecoderInfo)
    {
        this.definitionLevelValuesDecoderInfos.add(definitionLevelValuesDecoderInfo);
    }

    public List<DefinitionLevelValuesDecoderInfo> getDLValuesDecoderInfos()
    {
        return definitionLevelValuesDecoderInfos;
    }

    public int[] getRepetitionLevels()
    {
        return repetitionLevels;
    }

    public void setRepetitionLevels(int[] repetitionLevels)
    {
        this.repetitionLevels = repetitionLevels;
    }
}
