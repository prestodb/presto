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
package com.facebook.presto.spi.type;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

/**
 * VariableWidthType is a type that can have a different size for every value.
 */
public interface VariableWidthType
        extends Type
{
    /**
     * Writes the Slice value into the specified slice output.
     */
    int writeSlice(SliceOutput sliceOutput, Slice value, int offset, int length);
}
