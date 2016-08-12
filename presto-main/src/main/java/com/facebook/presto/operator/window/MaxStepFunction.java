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

package com.facebook.presto.operator.window;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.ValueWindowFunction;
import com.facebook.presto.spi.function.WindowFunctionSignature;

import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;

@WindowFunctionSignature(name = "max_step", returnType = "integer", argumentTypes = {"varchar", "varchar"})
public class MaxStepFunction extends ValueWindowFunction
{
  private static final String delimiter = "=>";
  private int actionChannel;
  private int patternDefChannel;
  private int patternItemsSize = 0;
  private int index = 0;
  private String currentSearchingItem = null;
  private String[] patternItems = null;

  public MaxStepFunction(List<Integer> argumentChannels)
  {
    this.actionChannel = argumentChannels.get(0);
    this.patternDefChannel = argumentChannels.get(1);
  }

  public void reset()
  {
    this.currentSearchingItem = null;
    this.index = 0;
  }

  @Override
  public void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition)
  {
    if (this.patternItems == null) {
      Slice patternDefSlice = windowIndex.getSlice(patternDefChannel, currentPosition);
      String patternDef = new String(patternDefSlice.getBytes());
      this.patternItems = patternDef.split(delimiter);
      this.patternItemsSize = patternItems.length;
    }

    Slice valueSlice = windowIndex.getSlice(actionChannel, currentPosition);
    String action = new String(valueSlice.getBytes());
    if (this.index < this.patternItemsSize) {
      this.currentSearchingItem = this.patternItems[this.index];
      if (this.currentSearchingItem.equals(action)) {
        this.index += 1;
      }
    }
    INTEGER.writeLong(output, this.index);
  }
}
