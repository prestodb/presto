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
package com.facebook.presto.spark.classloader_interface;

import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PrestoSparkTaskInputs
{
    // planNodeId -> Iterator<[partitionId, page]>
    private final Map<String, Iterator<Tuple2<Integer, PrestoSparkRow>>> inputsMap;

    public PrestoSparkTaskInputs(Map<String, Iterator<Tuple2<Integer, PrestoSparkRow>>> inputsMap)
    {
        this.inputsMap = requireNonNull(inputsMap, "inputs is null");
    }

    public Map<String, Iterator<Tuple2<Integer, PrestoSparkRow>>> getInputsMap()
    {
        return inputsMap;
    }
}
