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

// Utilities for dealing with nulls and other common aspects of expression
// over decoded Blocks.
public class TestExprContext
{
    IntArrayAllocator intArrayAllocator = new IntArrayAllocator();
    boolean[] nullsInReserve;
    boolean[] nullsInBatch;


    static void boolArrayOr(boolean[] target, boolean[] source, int[] map, int positionCount) {
      if (map == null) {
	  int i = 0;
	  /*
	  int bytesInWords = positionCount & ~7;
	  for (; i < bytesInWords; i += 8) {
	      unsafe.putLong(target, 16, unsafe.getLong(target, 16 + i) |
			     unsafe.getLong(source, 16 + i));
	  }
	  */
	  for (; i < positionCount; ++i) {
          target[i] |= source[i];
        }
      } else {
        for (int i = 0; i < positionCount; ++i) {
          target[i] |= source[map[i]];
        }
      }
    }

    void addNullFlags(boolean[] nullFlags, int[] map, int positionCount)
    {
      if (nullFlags != null) {
        if (nullsInBatch == null && map == null) {
          nullsInBatch = nullFlags;
        } else {
          boolean[]  newNulls;
          if (nullsInReserve !=null && nullsInReserve.length >= positionCount) {
            newNulls = nullsInReserve;
          } else {
            newNulls = new boolean[positionCount];
            nullsInReserve = newNulls;
          }
          if (nullsInBatch != null) {
            System.arraycopy(nullsInBatch, 0, newNulls, 0, positionCount);

          }
            nullsInBatch = newNulls;
            boolArrayOr(nullsInBatch, nullFlags, map, positionCount);
            }
          }
    }
}

