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
package com.facebook.presto.connector.thrift.util;

import com.facebook.presto.connector.thrift.ThriftColumnHandle;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;

import static com.facebook.presto.connector.thrift.api.PrestoThriftDomain.fromDomain;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public final class TupleDomainConversion
{
    private TupleDomainConversion() {}

    public static PrestoThriftTupleDomain tupleDomainToThriftTupleDomain(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (!tupleDomain.getDomains().isPresent()) {
            return new PrestoThriftTupleDomain(null);
        }
        return new PrestoThriftTupleDomain(tupleDomain.getDomains().get()
                .entrySet().stream()
                .collect(toImmutableMap(
                        entry -> ((ThriftColumnHandle) entry.getKey()).getColumnName(),
                        entry -> fromDomain(entry.getValue()))));
    }
}
