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
package com.facebook.presto.hive.zorder;

import java.util.Objects;

/**
 * The ZAddressRange class contains a minimum and maximum address. Both are inclusive.
 */
public class ZAddressRange<T>
{
    private final T minimumAddress;
    private final T maximumAddress;

    public ZAddressRange(T minimumAddress, T maximumAddress)
    {
        this.minimumAddress = minimumAddress;
        this.maximumAddress = maximumAddress;
    }

    public T getMinimumAddress()
    {
        return minimumAddress;
    }

    public T getMaximumAddress()
    {
        return maximumAddress;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o || this.hashCode() == o.hashCode()) {
            return true;
        }
        if (!(o instanceof ZAddressRange)) {
            return false;
        }
        ZAddressRange that = (ZAddressRange) o;
        return this.minimumAddress.equals(that.minimumAddress) && this.maximumAddress.equals(that.maximumAddress);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minimumAddress, maximumAddress);
    }
}
