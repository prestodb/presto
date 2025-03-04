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
package com.facebook.presto.router.scheduler;

import com.facebook.presto.spi.RouterPlugin;
import com.facebook.presto.spi.router.SchedulerFactory;

import java.util.ArrayList;
import java.util.List;

public class RouterSchedulerPlugin
        implements RouterPlugin
{
    @Override
    public Iterable<SchedulerFactory> getSchedulerFactories()
    {
        List<SchedulerFactory> schedulerFactories = new ArrayList<>();
        schedulerFactories.add(new MetricsBasedSchedulerFactory());
        return schedulerFactories;
    }
}
