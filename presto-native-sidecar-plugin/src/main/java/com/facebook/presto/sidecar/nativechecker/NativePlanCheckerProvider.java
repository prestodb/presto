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

package com.facebook.presto.sidecar.nativechecker;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.plan.PlanChecker;
import com.facebook.presto.spi.plan.PlanCheckerProvider;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class NativePlanCheckerProvider
        implements PlanCheckerProvider
{
    private final NodeManager nodeManager;
    private final JsonCodec<SimplePlanFragment> planFragmentJsonCodec;
    private final NativePlanCheckerConfig config;

    @Inject
    public NativePlanCheckerProvider(NodeManager nodeManager, JsonCodec<SimplePlanFragment> planFragmentJsonCodec, NativePlanCheckerConfig config)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.planFragmentJsonCodec = requireNonNull(planFragmentJsonCodec, "planFragmentJsonCodec is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public List<PlanChecker> getFragmentPlanCheckers()
    {
        return config.isPlanValidationEnabled() ?
                ImmutableList.of(new NativePlanChecker(nodeManager, planFragmentJsonCodec)) :
                ImmutableList.of();
    }
}
