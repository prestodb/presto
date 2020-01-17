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
package com.facebook.presto.failureDetector;

import com.facebook.airlift.discovery.client.ServiceDescriptor;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class NoOpFailureDetector
        implements FailureDetector
{
    @Override
    public Set<ServiceDescriptor> getFailed()
    {
        return ImmutableSet.of();
    }

    @Override
    public State getState(HostAddress hostAddress)
    {
        return State.UNKNOWN;
    }
}
