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
package io.prestosql.operator;

import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.prestosql.spi.Page;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public final class ArrayPositionLinks
        implements PositionLinks
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayPositionLinks.class).instanceSize();

    public static class FactoryBuilder
            implements PositionLinks.FactoryBuilder
    {
        private final int[] positionLinks;
        private int size;

        private FactoryBuilder(int size)
        {
            positionLinks = new int[size];
            Arrays.fill(positionLinks, -1);
        }

        @Override
        public int link(int left, int right)
        {
            size++;
            positionLinks[left] = right;
            return left;
        }

        @Override
        public PositionLinks.Factory build()
        {
            return new PositionLinks.Factory()
            {
                @Override
                public PositionLinks create(List<JoinFilterFunction> searchFunctions)
                {
                    return new ArrayPositionLinks(positionLinks);
                }

                @Override
                public long checksum()
                {
                    return XxHash64.hash(Slices.wrappedIntArray(positionLinks));
                }
            };
        }

        @Override
        public int size()
        {
            return size;
        }
    }

    private final int[] positionLinks;

    private ArrayPositionLinks(int[] positionLinks)
    {
        this.positionLinks = requireNonNull(positionLinks, "positionLinks is null");
    }

    public static FactoryBuilder builder(int size)
    {
        return new FactoryBuilder(size);
    }

    @Override
    public int start(int position, int probePosition, Page allProbeChannelsPage)
    {
        return position;
    }

    @Override
    public int next(int position, int probePosition, Page allProbeChannelsPage)
    {
        return positionLinks[position];
    }

    @Override
    public long getSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(positionLinks);
    }
}
