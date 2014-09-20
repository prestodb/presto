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
package com.facebook.presto.hive.orc.stream;

import java.io.IOException;

public interface LongStream
{
    long next()
            throws IOException;

    void nextIntVector(int items, int[] vector)
            throws IOException;

    void nextIntVector(int items, int[] vector, boolean[] isNull)
            throws IOException;

    void nextLongVector(int items, long[] vector)
            throws IOException;

    void nextLongVector(int items, long[] vector, boolean[] isNull)
            throws IOException;

    void skip(int items)
            throws IOException;

    long sum(int items)
            throws IOException;
}
