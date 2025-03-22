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
package com.facebook.presto.experimental;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.experimental.auto_gen.ThriftTaskState;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ThriftTaskStateUtils
{
    private static final Logger log = Logger.get(ThriftTaskStateUtils.class);

    private ThriftTaskStateUtils() {}

    public static final Set<ThriftTaskState> TERMINAL_TASK_STATES = Stream.of(
            ThriftTaskState.FINISHED,
            ThriftTaskState.CANCELED,
            ThriftTaskState.ABORTED,
            ThriftTaskState.FINISHED).collect(Collectors.toSet());
    private static final Map<Integer, TaskState> CODE_TO_TASK_STATE = Arrays.stream(TaskState.values()).collect(Collectors.toMap(TaskState::getCode, Function.identity()));

    public static boolean isDone(ThriftTaskState state)
    {
        return TERMINAL_TASK_STATES.contains(state);
    }

    public static TaskState toTaskState(ThriftTaskState state)
    {
        return CODE_TO_TASK_STATE.get(state.getValue());
    }

    public static ThriftTaskState fromTaskState(TaskState state)
    {
        return ThriftTaskState.findByValue(state.getCode());
    }
}
