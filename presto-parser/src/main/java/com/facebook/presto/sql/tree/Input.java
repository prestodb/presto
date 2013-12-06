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
package com.facebook.presto.sql.tree;

import com.google.common.base.Function;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Represents a reference to a field in a physical execution plan
 * <p/>
 * TODO: This class belongs to the execution engine and should not be in this package.
 */
public class Input
{
    private final int channel;
    private final int field;

    public Input(int channel, int field)
    {
        checkArgument(field == 0);
        this.channel = channel;
        this.field = field;
    }

    public int getChannel()
    {
        return channel;
    }

    public int getField()
    {
        return field;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(channel, field);
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
        final Input other = (Input) obj;
        return Objects.equal(this.channel, other.channel) && Objects.equal(this.field, other.field);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("channel", channel)
                .add("field", field)
                .toString();
    }

    public static Function<Input, Integer> channelGetter()
    {
        return new Function<Input, Integer>()
        {
            @Override
            public Integer apply(Input input)
            {
                return input.getChannel();
            }
        };
    }

    public static Function<Input, Integer> fieldGetter()
    {
        return new Function<Input, Integer>()
        {
            @Override
            public Integer apply(Input input)
            {
                return input.getField();
            }
        };
    }
}
