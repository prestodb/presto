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
package com.facebook.presto.sql.planner.optimizations;

/**
 * Translates the planning intent for an RPC function call into its resolved execution
 * properties (streaming mode, and in future dispatch parallelism / batch size, etc.).
 *
 * The engine builds an {@link RpcExecutionIntent} (requested mode + input stats + session) and
 * delegates the decision to this policy, so a deployment can plug in a richer, stats-driven
 * strategy without changing the engine. Both the intent and the returned
 * {@link RpcExecutionProperties} are growable value objects, so new signals or knobs can be
 * added without changing this method signature. The decision is made at plan time, so the
 * returned streaming mode must be concrete (never AUTOMATIC).
 *
 * The default implementation ({@link DefaultRpcExecutionPolicy}) carries no batching
 * heuristic; bind an override to enable AUTOMATIC.
 */
public interface RpcExecutionPolicy
{
    /**
     * @param intent the planning intent for one RPC call (requested mode, input stats, session)
     * @return the resolved execution properties; the streaming mode must be concrete (never
     *     AUTOMATIC)
     */
    RpcExecutionProperties translateIntent(RpcExecutionIntent intent);
}
