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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.storage.BucketBalancer;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static java.util.Objects.requireNonNull;

public class TriggerBucketBalancerProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle TRIGGER_BUCKET_BALANCER = methodHandle(
            TriggerBucketBalancerProcedure.class,
            "createTriggerBucketBalancer");

    private final BucketBalancer bucketBalancer;

    @Inject
    public TriggerBucketBalancerProcedure(BucketBalancer bucketBalancer)
    {
        this.bucketBalancer = requireNonNull(bucketBalancer, "bucketBalancer is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "trigger_bucket_balancer",
                ImmutableList.of(),
                TRIGGER_BUCKET_BALANCER.bindTo(this));
    }

    public void createTriggerBucketBalancer()
    {
        bucketBalancer.startBalanceJob();
    }
}
