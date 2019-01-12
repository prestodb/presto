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
package io.prestosql.execution.scheduler;

import io.prestosql.spi.HostAddress;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

/**
 * Implementations of this interface must be thread safe.
 */
@ThreadSafe
public interface NetworkTopology
{
    NetworkLocation locate(HostAddress address);

    /**
     * Strings describing the meaning of each segment of a NetworkLocation returned from locate().
     * This method must return a constant.
     */
    List<String> getLocationSegmentNames();
}
