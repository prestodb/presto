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
package com.facebook.presto.execution.buffer;

import com.facebook.drift.annotations.ThriftEnum;
import com.facebook.drift.annotations.ThriftEnumValue;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

@ThriftEnum
public enum BufferState
{
    /**
     * Additional buffers can be added.
     * Any next state is allowed.
     */
    OPEN(true, true, false, 0),
    /**
     * No more buffers can be added.
     * Next state is {@link #FLUSHING}.
     */
    NO_MORE_BUFFERS(true, false, false, 1),
    /**
     * No more pages can be added.
     * Next state is {@link #FLUSHING}.
     */
    NO_MORE_PAGES(false, true, false, 2),
    /**
     * No more pages or buffers can be added, and buffer is waiting
     * for the final pages to be consumed.
     * Next state is {@link #FINISHED}.
     */
    FLUSHING(false, false, false, 3),
    /**
     * No more buffers can be added and all pages have been consumed.
     * This is the terminal state.
     */
    FINISHED(false, false, true, 4),
    /**
     * Buffer has failed.  No more buffers or pages can be added.  Readers
     * will be blocked, as to not communicate a finished state.  It is
     * assumed that the reader will be cleaned up elsewhere.
     * This is the terminal state.
     */
    FAILED(false, false, true, 5);

    public static final Set<BufferState> TERMINAL_BUFFER_STATES = Stream.of(BufferState.values()).filter(BufferState::isTerminal).collect(toImmutableSet());

    private final boolean newPagesAllowed;
    private final boolean newBuffersAllowed;
    private final boolean terminal;
    private final int value;

    BufferState(boolean newPagesAllowed, boolean newBuffersAllowed, boolean terminal, int value)
    {
        this.newPagesAllowed = newPagesAllowed;
        this.newBuffersAllowed = newBuffersAllowed;
        this.terminal = terminal;
        this.value = value;
    }

    public boolean canAddPages()
    {
        return newPagesAllowed;
    }

    public boolean canAddBuffers()
    {
        return newBuffersAllowed;
    }

    public boolean isTerminal()
    {
        return terminal;
    }

    @ThriftEnumValue
    public int getValue()
    {
        return value;
    }
}
