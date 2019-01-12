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
package io.prestosql.execution.executor;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Task (and split) priority is composed of a level and a within-level
 * priority. Level decides which queue the split is placed in, while
 * within-level priority decides which split is executed next in that level.
 * <p>
 * Tasks move from a lower to higher level as they exceed level thresholds
 * of total scheduled time accrued to a task.
 * <p>
 * The priority within a level increases with the scheduled time accumulated
 * in that level. This is necessary to achieve fairness when tasks acquire
 * scheduled time at varying rates.
 * <p>
 * However, this priority is <b>not</b> equal to the task total accrued
 * scheduled time. When a task graduates to a higher level, the level
 * priority is set to the minimum current priority in the new level. This
 * allows us to maintain instantaneous fairness in terms of scheduled time.
 */
@Immutable
public final class Priority
{
    private final int level;
    private final long levelPriority;

    public Priority(int level, long levelPriority)
    {
        this.level = level;
        this.levelPriority = levelPriority;
    }

    public int getLevel()
    {
        return level;
    }

    public long getLevelPriority()
    {
        return levelPriority;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("level", level)
                .add("levelPriority", levelPriority)
                .toString();
    }
}
