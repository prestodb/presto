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

import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.WindowFrame;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FrameInfo
{
    private final WindowFrame.Type type;
    private final FrameBound.Type startType;
    private final int startChannel;
    private final FrameBound.Type endType;
    private final int endChannel;

    public FrameInfo(
            WindowFrame.Type type,
            FrameBound.Type startType,
            Optional<Integer> startChannel,
            FrameBound.Type endType,
            Optional<Integer> endChannel)
    {
        this.type = requireNonNull(type, "type is null");
        this.startType = requireNonNull(startType, "startType is null");
        this.startChannel = requireNonNull(startChannel, "startChannel is null").orElse(-1);
        this.endType = requireNonNull(endType, "endType is null");
        this.endChannel = requireNonNull(endChannel, "endChannel is null").orElse(-1);
    }

    public WindowFrame.Type getType()
    {
        return type;
    }

    public FrameBound.Type getStartType()
    {
        return startType;
    }

    public int getStartChannel()
    {
        return startChannel;
    }

    public FrameBound.Type getEndType()
    {
        return endType;
    }

    public int getEndChannel()
    {
        return endChannel;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, startType, startChannel, endType, endChannel);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FrameInfo other = (FrameInfo) obj;

        return Objects.equals(this.type, other.type) &&
                Objects.equals(this.startType, other.startType) &&
                Objects.equals(this.startChannel, other.startChannel) &&
                Objects.equals(this.endType, other.endType) &&
                Objects.equals(this.endChannel, other.endChannel);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("startType", startType)
                .add("startChannel", startChannel)
                .add("endType", endType)
                .add("endChannel", endChannel)
                .toString();
    }
}
