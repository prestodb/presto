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
package com.facebook.presto.execution.executor;

public enum QueryRecoveryState
{
    INIT_GRACEFUL_PREEMPTION,
    WAITING_FOR_OUTPUT_BUFFER,
    WAITING_FOR_RUNNING_SPLITS,
    WAITING_FOR_BLOCKED_SPLITS,
    INITIATE_HANDLE_SHUTDOWN
}
