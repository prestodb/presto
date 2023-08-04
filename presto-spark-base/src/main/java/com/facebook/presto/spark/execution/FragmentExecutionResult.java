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
package com.facebook.presto.spark.execution;

import com.facebook.presto.spark.RddAndMore;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import org.apache.spark.MapOutputStatistics;
import org.apache.spark.SimpleFutureAction;

import java.util.Optional;

/*
    Represents result of a fragment/sub-plan's execution.
    It contains RDD, execution stats and methods to extract result.
 */
public class FragmentExecutionResult<T extends PrestoSparkTaskOutput>
{
    private final RddAndMore<T> rddAndMore;
    private final Optional<SimpleFutureAction<MapOutputStatistics>> mapOutputStatisticsFutureAction;

    public FragmentExecutionResult(RddAndMore<T> rddAndMore, Optional<SimpleFutureAction<MapOutputStatistics>> mapOutputStatisticsFutureAction)
    {
        this.rddAndMore = rddAndMore;
        this.mapOutputStatisticsFutureAction = mapOutputStatisticsFutureAction;
    }

    public RddAndMore<T> getRddAndMore()
    {
        return rddAndMore;
    }

    public Optional<SimpleFutureAction<MapOutputStatistics>> getMapOutputStatisticsFutureAction()
    {
        return mapOutputStatisticsFutureAction;
    }
}
