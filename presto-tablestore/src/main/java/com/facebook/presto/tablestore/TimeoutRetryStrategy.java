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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.DefaultRetryStrategy;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TimeoutRetryStrategy
        extends DefaultRetryStrategy
{
    private final int timeoutSeconds;

    public TimeoutRetryStrategy(int timeoutSeconds)
    {
        super(timeoutSeconds, SECONDS);
        this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    protected boolean isIdempotent(String action)
    {
        return true;
    }

    @Override
    public RetryStrategy clone()
    {
        return new TimeoutRetryStrategy(timeoutSeconds);
    }
}
