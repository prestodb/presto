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
package com.facebook.presto.geospatial.rtree;

import com.facebook.presto.geospatial.Rectangle;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class RtreeTestUtils
{
    private RtreeTestUtils() {}

    public static List<Rectangle> makeRectangles(Random random, int numRectangles)
    {
        return IntStream.range(0, numRectangles)
                .mapToObj(i -> makeRectangle(random))
                .collect(Collectors.toList());
    }

    /*
     * Make a random rectangle at a random origin of size < 10.
     */
    private static Rectangle makeRectangle(Random random)
    {
        double minX = randomDouble(random, -100, 100);
        double minY = randomDouble(random, -100, 100);
        double sizeX = randomDouble(random, 0.0, 10);
        double sizeY = randomDouble(random, 0.0, 10);
        return new Rectangle(minX, minY, minX + sizeX, minY + sizeY);
    }

    private static double randomDouble(Random random, double min, double max)
    {
        return min + random.nextDouble() * (max - min);
    }
}
