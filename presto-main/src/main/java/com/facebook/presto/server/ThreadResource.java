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
package com.facebook.presto.server;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Comparator;
import java.util.List;

import static com.facebook.presto.server.ThreadResource.Info.byName;
import static com.facebook.presto.server.security.RoleType.ADMIN;

@Path("/")
@RolesAllowed(ADMIN)
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
            if (info == null) {
                continue;
            }
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

    @ThriftStruct
    public static class Info
    {
        private final long id;
        private final String name;
        private final String state;
        private final Long lockOwnerId;
        private final List<StackLine> stackTrace;

        @ThriftConstructor
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

        @ThriftField(1)
        @JsonProperty
        public long getId()
        {
            return id;
        }

        @ThriftField(2)
        @JsonProperty
        public String getName()
        {
            return name;
        }

        @ThriftField(3)
        @JsonProperty
        public String getState()
        {
            return state;
        }

        @ThriftField(4)
        @JsonProperty
        public Long getLockOwnerId()
        {
            return lockOwnerId;
        }

        @ThriftField(5)
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

    @ThriftStruct
    public static class StackLine
    {
        private final String file;
        private final int line;
        private final String className;
        private final String method;

        @ThriftConstructor
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

        @ThriftField(1)
        @JsonProperty
        public String getFile()
        {
            return file;
        }

        @ThriftField(2)
        @JsonProperty
        public int getLine()
        {
            return line;
        }

        @ThriftField(3)
        @JsonProperty
        public String getClassName()
        {
            return className;
        }

        @ThriftField(4)
        @JsonProperty
        public String getMethod()
        {
            return method;
        }
    }
}
