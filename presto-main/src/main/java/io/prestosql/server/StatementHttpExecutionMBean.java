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

import io.airlift.concurrent.ThreadPoolExecutorMBean;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.Objects.requireNonNull;

public class StatementHttpExecutionMBean
{
    private final ThreadPoolExecutorMBean responseExecutor;
    private final ThreadPoolExecutorMBean timeoutExecutor;

    @Inject
    public StatementHttpExecutionMBean(@ForStatementResource ExecutorService responseExecutor, @ForStatementResource ScheduledExecutorService timeoutExecutor)
    {
        requireNonNull(responseExecutor, "responseExecutor is null");
        requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.responseExecutor = new ThreadPoolExecutorMBean((ThreadPoolExecutor) responseExecutor);
        this.timeoutExecutor = new ThreadPoolExecutorMBean((ThreadPoolExecutor) timeoutExecutor);
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getResponseExecutor()
    {
        return responseExecutor;
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getTimeoutExecutor()
    {
        return timeoutExecutor;
    }
}
