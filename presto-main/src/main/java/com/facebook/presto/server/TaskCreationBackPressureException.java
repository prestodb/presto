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

public class TaskCreationBackPressureException
        extends RuntimeException
{
    private final int retryAfter;

    public TaskCreationBackPressureException(int retryAfter)
    {
        super("Server returned TOO_MANY_REQUESTS to backpressure task creation, to retry after: " + retryAfter + "seconds");
        this.retryAfter = retryAfter;
    }

    public int getRetryAfter()
    {
        return retryAfter;
    }
}
