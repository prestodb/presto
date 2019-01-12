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
package io.prestosql.operator.project;

import com.google.common.primitives.Ints;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class InputChannels
{
    private final int[] inputChannels;

    public InputChannels(int... inputChannels)
    {
        this.inputChannels = inputChannels.clone();
    }

    public InputChannels(List<Integer> inputChannels)
    {
        this.inputChannels = inputChannels.stream().mapToInt(Integer::intValue).toArray();
    }

    public int size()
    {
        return inputChannels.length;
    }

    public List<Integer> getInputChannels()
    {
        return Collections.unmodifiableList(Ints.asList(inputChannels));
    }

    public Page getInputChannels(Page page)
    {
        Block[] blocks = new Block[inputChannels.length];
        for (int i = 0; i < inputChannels.length; i++) {
            blocks[i] = page.getBlock(inputChannels[i]);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(Arrays.toString(inputChannels))
                .toString();
    }
}
