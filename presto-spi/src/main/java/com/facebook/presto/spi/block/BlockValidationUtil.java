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
package com.facebook.presto.spi.block;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

final class BlockValidationUtil
{
    private BlockValidationUtil()
    {
    }

    static void checkValidPositions(List<Integer> positions, int positionCount)
    {
        Set<Integer> invalidPositions = positions.stream().filter(position -> position >= positionCount).collect(toSet());
        if (!invalidPositions.isEmpty()) {
            throw new IllegalArgumentException("Invalid positions " + invalidPositions + " in block with " + positionCount + " positions");
        }
    }
}
