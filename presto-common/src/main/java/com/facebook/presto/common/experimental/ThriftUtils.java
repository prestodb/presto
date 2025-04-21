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
package com.facebook.presto.common.experimental;

import com.facebook.airlift.stats.Distribution;
import com.facebook.presto.common.experimental.auto_gen.ThriftDistributionSnapshot;
import com.facebook.presto.common.experimental.auto_gen.ThriftDuration;
import com.facebook.presto.common.experimental.auto_gen.ThriftTimeUnit;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class ThriftUtils
{
    private ThriftUtils() {}

    public static Distribution.DistributionSnapshot fromThriftDistributionSnapshot(ThriftDistributionSnapshot thriftSnapshot)
    {
        return new Distribution.DistributionSnapshot(
                thriftSnapshot.getMaxError(),
                thriftSnapshot.getCount(),
                thriftSnapshot.getTotal(),
                thriftSnapshot.getP01(),
                thriftSnapshot.getP05(),
                thriftSnapshot.getP10(),
                thriftSnapshot.getP25(),
                thriftSnapshot.getP50(),
                thriftSnapshot.getP75(),
                thriftSnapshot.getP90(),
                thriftSnapshot.getP95(),
                thriftSnapshot.getP99(),
                thriftSnapshot.getMin(),
                thriftSnapshot.getMax(),
                thriftSnapshot.getAvg());
    }

    public static ThriftDistributionSnapshot toThriftDistributionSnapshot(Distribution.DistributionSnapshot snapshot)
    {
        return new ThriftDistributionSnapshot(
                snapshot.getMaxError(),
                snapshot.getCount(),
                snapshot.getTotal(),
                snapshot.getP01(),
                snapshot.getP05(),
                snapshot.getP10(),
                snapshot.getP25(),
                snapshot.getP50(),
                snapshot.getP75(),
                snapshot.getP90(),
                snapshot.getP95(),
                snapshot.getP99(),
                snapshot.getMin(),
                snapshot.getMax(),
                snapshot.getAvg());
    }

    public static Duration fromThriftDuration(ThriftDuration thriftDuration)
    {
        return new Duration(thriftDuration.getValue(), TimeUnit.valueOf(thriftDuration.getUnit().name()));
    }

    public static ThriftDuration toThriftDuration(Duration duration)
    {
        return new ThriftDuration(duration.getValue(), ThriftTimeUnit.valueOf(duration.getUnit().name()));
    }
}
