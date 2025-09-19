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
package com.facebook.presto.thrift.codec.utils;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.spi.ColumnHandle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThriftClassUtils
{
    private ThriftClassUtils()
    {}

    public static ThriftTupleDomain toThriftTupleDomain(TupleDomain<ColumnHandle> tupleDomain)
    {
        Map<String, ThriftDomain> thriftDomains = new HashMap<>();

        tupleDomain.getDomains().ifPresent(domains -> {
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                ColumnHandle columnName = entry.getKey();
                Domain domain = entry.getValue();

                ThriftDomain thriftDomain = new ThriftDomain();
                thriftDomain.setNullAllowed(domain.isNullAllowed());

                thriftDomain.setRanges(domain.getValues().getRanges().getOrderedRanges());
                thriftDomains.put("", thriftDomain);
            }
        });

        return new ThriftTupleDomain(thriftDomains);
    }

    public static TupleDomain<ColumnHandle> fromThriftTupleDomain(
            ThriftTupleDomain thriftTupleDomain)
    {
        Map<ColumnHandle, Domain> domainMap = new HashMap<>();

        for (Map.Entry<String, ThriftDomain> entry : thriftTupleDomain.getDomains().entrySet()) {
            ColumnHandle columnHandle = null;
            ThriftDomain thriftDomain = entry.getValue();

            List<Range> ranges = thriftDomain.getRanges();
            ValueSet valueSet = ValueSet.ofRanges(ranges);
            Domain domain = Domain.create(valueSet, thriftDomain.isNullAllowed());

            domainMap.put(columnHandle, domain);
        }

        return TupleDomain.withColumnDomains(domainMap);
    }
}
