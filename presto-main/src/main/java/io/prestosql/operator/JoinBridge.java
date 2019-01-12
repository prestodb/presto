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
package io.prestosql.operator;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * A bridge that connects build, probe, and outer operators of a join.
 * It often carries data that lets probe find out what is available on
 * the build side, and lets outer find the orphaned rows.
 */
public interface JoinBridge
{
    /**
     * Can be called only after build and probe are finished.
     */
    OuterPositionIterator getOuterPositionIterator();

    void destroy();

    ListenableFuture<?> whenBuildFinishes();
}
