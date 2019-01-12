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
package io.prestosql.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Comparator;
import java.util.List;

import static io.prestosql.server.ThreadResource.Info.byName;

@Path("/")
public class ThreadResource
{
    @GET
    @Path("/v1/thread")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Info> getThreadInfo()
    {
        ThreadMXBean mbean = ManagementFactory.getThreadMXBean();

        ImmutableList.Builder<Info> builder = ImmutableList.builder();
        for (ThreadInfo info : mbean.getThreadInfo(mbean.getAllThreadIds(), Integer.MAX_VALUE)) {
            builder.add(new Info(
                    info.getThreadId(),
                    info.getThreadName(),
                    info.getThreadState().name(),
                    info.getLockOwnerId() == -1 ? null : info.getLockOwnerId(),
                    toStackTrace(info.getStackTrace())));
        }
        return Ordering.from(byName()).sortedCopy(builder.build());
    }

    private static List<StackLine> toStackTrace(StackTraceElement[] stackTrace)
    {
        ImmutableList.Builder<StackLine> builder = ImmutableList.builder();

        for (StackTraceElement item : stackTrace) {
            builder.add(new StackLine(
                    item.getFileName(),
                    item.getLineNumber(),
                    item.getClassName(),
                    item.getMethodName()));
        }

        return builder.build();
    }

    public static class Info
    {
        private final long id;
        private final String name;
        private final String state;
        private final Long lockOwnerId;
        private final List<StackLine> stackTrace;

        @JsonCreator
        public Info(
                @JsonProperty("id") long id,
                @JsonProperty("name") String name,
                @JsonProperty("state") String state,
                @JsonProperty("lockOwnerId") Long lockOwnerId,
                @JsonProperty("stackTrace") List<StackLine> stackTrace)
        {
            this.id = id;
            this.name = name;
            this.state = state;
            this.lockOwnerId = lockOwnerId;
            this.stackTrace = stackTrace;
        }

        @JsonProperty
        public long getId()
        {
            return id;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public String getState()
        {
            return state;
        }

        @JsonProperty
        public Long getLockOwnerId()
        {
            return lockOwnerId;
        }

        @JsonProperty
        public List<StackLine> getStackTrace()
        {
            return stackTrace;
        }

        public static Comparator<Info> byName()
        {
            return new Comparator<Info>()
            {
                @Override
                public int compare(Info info, Info info2)
                {
                    return info.getName().compareTo(info2.getName());
                }
            };
        }
    }

    public static class StackLine
    {
        private final String file;
        private final int line;
        private final String className;
        private final String method;

        @JsonCreator
        public StackLine(
                @JsonProperty("file") String file,
                @JsonProperty("line") int line,
                @JsonProperty("className") String className,
                @JsonProperty("method") String method)
        {
            this.file = file;
            this.line = line;
            this.className = className;
            this.method = method;
        }

        @JsonProperty
        public String getFile()
        {
            return file;
        }

        @JsonProperty
        public int getLine()
        {
            return line;
        }

        @JsonProperty
        public String getClassName()
        {
            return className;
        }

        @JsonProperty
        public String getMethod()
        {
            return method;
        }
    }
}
