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
package com.facebook.presto.orc.reader.vector;

import com.facebook.presto.common.predicate.vector.TupleDomainFilterVector;
import com.facebook.presto.orc.stream.DoubleInputStream;

import java.io.IOException;

public interface DoubleVectorSelectiveReadAndFilter
{
    int firstLevelReadAndFilter(TupleDomainFilterVector filter, DoubleInputStream inputStream,
            int[] positions, int positionCount, int[] outputPositions)
            throws IOException;

    int firstLevelReadAndFilterWithOutputRequired(TupleDomainFilterVector filter, DoubleInputStream inputStream,
            int[] positions, int positionCount, int[] outputPositions, long[] values)
            throws IOException;
}
