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
package com.facebook.presto.orc;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

public class TupleDomainConfig
{
    private int notInThreshold = 1000;

    public int getNotInThreshold()
    {
        return notInThreshold;
    }

    @Config("orc.tuple-domain.not-in-threshold")
    @ConfigDescription("Define tuple domain not in optimization threshold")
    public TupleDomainConfig setNotInThreshold(int notInThreshold)
    {
        this.notInThreshold = notInThreshold;
        return this;
    }
}
