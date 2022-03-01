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
package com.facebook.presto.orc.metadata;

public enum CompressionKind
{
    NONE(0),
    ZLIB(9),
    SNAPPY(17),
    LZ4(14),
    ZSTD(21);

    /**
     * Minimum size of the input data that would benefit from compression.
     */
    private final int minCompressibleSize;

    CompressionKind(int minCompressibleSize)
    {
        this.minCompressibleSize = minCompressibleSize;
    }

    public int getMinCompressibleSize()
    {
        return minCompressibleSize;
    }
}
