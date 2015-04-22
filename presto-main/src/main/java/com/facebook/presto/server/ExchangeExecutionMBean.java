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

import com.facebook.presto.operator.ForExchange;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;

public class ExchangeExecutionMBean
{
    private final ThreadPoolExecutorMBean executorMBean;

    @Inject
    public ExchangeExecutionMBean(@ForExchange ScheduledExecutorService executor)
    {
        checkNotNull(executor, "executor is null");
        this.executorMBean = new ThreadPoolExecutorMBean(checkType(executor, ThreadPoolExecutor.class, "executor"));
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }
}
