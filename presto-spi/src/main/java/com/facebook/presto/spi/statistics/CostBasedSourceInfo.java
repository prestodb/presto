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
package com.facebook.presto.spi.statistics;

/**
 * Describes plan statistics which are derived from cost based optimizer.
 */
public class CostBasedSourceInfo
        extends SourceInfo
{
    private boolean confident;

    public CostBasedSourceInfo(boolean confident)
    {
        this.confident = confident;
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        return getClass() == obj.getClass();
    }

    @Override
    public String toString()
    {
        return "CostBasedSourceInfo{}";
    }

    @Override
    public boolean isConfident()
    {
        return confident;
    }

    @Override
    public String getSourceInfoName()
    {
        return "CBO";
    }

    @Override
    public boolean estimateSizeUsingVariables()
    {
        return true;
    }
}
