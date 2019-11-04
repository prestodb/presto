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
package com.facebook.presto.spi.session;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public interface SessionLogger
{
    SessionLogger NOOP_SESSION_LOGGER = new SessionLogger()
    {
        private LinkedList<Entry> emptyQ = new LinkedList<>();

        @Override
        public void log(Supplier<String> message)
        {
        }

        @Override
        public Queue<Entry> getEntries()
        {
            return emptyQ;
        }
    };

    void log(Supplier<String> message);

    Queue<Entry> getEntries();

    @Immutable
    public static class Entry
    {
        final String message;
        final long nanos;
        final String threadName;

        @JsonCreator
        public Entry(@JsonProperty("message") String message, @JsonProperty("nanos") long nanos, @JsonProperty("threadName") String threadName)
        {
            this.message = requireNonNull(message, "messsage is null");
            this.nanos = nanos;
            this.threadName = threadName;
        }

        @JsonProperty
        public String getThreadName()
        {
            return threadName;
        }

        @JsonProperty
        public String getMessage()
        {
            return message;
        }

        @JsonProperty
        public long getNanos()
        {
            return nanos;
        }
    }
}
