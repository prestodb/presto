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

import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class CompressionParameters
{
    private final CompressionKind kind;
    private final OptionalInt level;
    private final int maxBufferSize;

    public CompressionParameters(CompressionKind kind, OptionalInt level, int maxBufferSize)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.level = requireNonNull(level, "level is null");
        this.maxBufferSize = maxBufferSize;
    }

    public CompressionKind getKind()
    {
        return kind;
    }

    public OptionalInt getLevel()
    {
        return level;
    }

    public int getMaxBufferSize()
    {
        return maxBufferSize;
    }
}
