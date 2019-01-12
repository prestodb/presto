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
package io.prestosql.execution.buffer;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

public enum BufferState
{
    /**
     * Additional buffers can be added.
     * Any next state is allowed.
     */
    OPEN(true, true, false),
    /**
     * No more buffers can be added.
     * Next state is {@link #FLUSHING}.
     */
    NO_MORE_BUFFERS(true, false, false),
    /**
     * No more pages can be added.
     * Next state is {@link #FLUSHING}.
     */
    NO_MORE_PAGES(false, true, false),
    /**
     * No more pages or buffers can be added, and buffer is waiting
     * for the final pages to be consumed.
     * Next state is {@link #FINISHED}.
     */
    FLUSHING(false, false, false),
    /**
     * No more buffers can be added and all pages have been consumed.
     * This is the terminal state.
     */
    FINISHED(false, false, true),
    /**
     * Buffer has failed.  No more buffers or pages can be added.  Readers
     * will be blocked, as to not communicate a finished state.  It is
     * assumed that the reader will be cleaned up elsewhere.
     * This is the terminal state.
     */
    FAILED(false, false, true);

    public static final Set<BufferState> TERMINAL_BUFFER_STATES = Stream.of(BufferState.values()).filter(BufferState::isTerminal).collect(toImmutableSet());

    private final boolean newPagesAllowed;
    private final boolean newBuffersAllowed;
    private final boolean terminal;

    BufferState(boolean newPagesAllowed, boolean newBuffersAllowed, boolean terminal)
    {
        this.newPagesAllowed = newPagesAllowed;
        this.newBuffersAllowed = newBuffersAllowed;
        this.terminal = terminal;
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
}
