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
package com.facebook.presto.execution.scheduler.clusterOverload;

/**
 * Listener interface for receiving notifications about cluster overload state changes.
 * This interface allows components to react to changes in cluster capacity and
 * resource availability, particularly when the cluster transitions from an overloaded
 * state back to a normal state.
 */
public interface ClusterOverloadStateListener
{
    /**
     * Called when the cluster enters an overloaded state.
     * This indicates that the cluster resources are under stress and
     * new query admissions should be restricted.
     */
    void onClusterEnteredOverloadedState();

    /**
     * Called when the cluster exits the overloaded state.
     * This indicates that cluster resources have recovered and
     * normal query processing can resume.
     */
    void onClusterExitedOverloadedState();
}
