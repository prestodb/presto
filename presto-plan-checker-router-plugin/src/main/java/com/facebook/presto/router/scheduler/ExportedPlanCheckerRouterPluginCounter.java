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
package com.facebook.presto.router.scheduler;

import com.facebook.airlift.stats.CounterStat;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

public class ExportedPlanCheckerRouterPluginCounter
        implements DynamicMBean
{
    private final CounterStat counter;

    public ExportedPlanCheckerRouterPluginCounter(CounterStat counter)
    {
        this.counter = counter;
    }

    @Override
    public Object getAttribute(String attribute)
            throws AttributeNotFoundException
    {
        if ("Count".equals(attribute)) {
            return counter.getTotalCount();
        }
        throw new AttributeNotFoundException("Unknown attribute: " + attribute);
    }

    @Override
    public AttributeList getAttributes(String[] attributes)
    {
        AttributeList list = new AttributeList();
        for (String attr : attributes) {
            try {
                list.add(new Attribute(attr, getAttribute(attr)));
            }
            catch (Exception ignored) {
            }
        }
        return list;
    }

    @Override
    public void setAttribute(Attribute attribute) {}

    @Override
    public AttributeList setAttributes(AttributeList attributes)
    {
        return new AttributeList();
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature)
    {
        return null;
    }

    @Override
    public MBeanInfo getMBeanInfo()
    {
        return new MBeanInfo(
                this.getClass().getName(),
                "Exported CounterStat",
                new MBeanAttributeInfo[] {
                        new MBeanAttributeInfo("Count", "long", "Total count", true, false, false)
                },
                null, null, null);
    }
}
