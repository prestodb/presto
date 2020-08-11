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

import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;

import java.io.ObjectInputStream;
import java.util.Set;

// TODO: move this to airlift or jmxutils

/**
 * MBeanServer wrapper that a ignores calls to registerMBean when there is already
 * a MBean registered with the specified object name.
 */
@ThreadSafe
public class RebindSafeMBeanServer
        implements MBeanServer
{
    private static final Logger log = Logger.get(RebindSafeMBeanServer.class);

    private final MBeanServer mbeanServer;

    public RebindSafeMBeanServer(MBeanServer mbeanServer)
    {
        this.mbeanServer = mbeanServer;
    }

    /**
     * Delegates to the wrapped mbean server, but if a mbean is already registered
     * with the specified name, the existing instance is returned.
     */
    @Override
    public ObjectInstance registerMBean(Object object, ObjectName name)
            throws MBeanRegistrationException, NotCompliantMBeanException
    {
        while (true) {
            try {
                // try to register the mbean
                return mbeanServer.registerMBean(object, name);
            }
            catch (InstanceAlreadyExistsException ignored) {
            }

            try {
                // a mbean is already installed, try to return the already registered instance
                ObjectInstance objectInstance = mbeanServer.getObjectInstance(name);
                log.debug("%s already bound to %s", name, objectInstance);
                return objectInstance;
            }
            catch (InstanceNotFoundException ignored) {
                // the mbean was removed before we could get the reference
                // start the whole process over again
            }
        }
    }

    @Override
    public void unregisterMBean(ObjectName name)
            throws InstanceNotFoundException, MBeanRegistrationException
    {
        mbeanServer.unregisterMBean(name);
    }

    @Override
    public ObjectInstance getObjectInstance(ObjectName name)
            throws InstanceNotFoundException
    {
        return mbeanServer.getObjectInstance(name);
    }

    @Override
    public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query)
    {
        return mbeanServer.queryMBeans(name, query);
    }

    @Override
    public Set<ObjectName> queryNames(ObjectName name, QueryExp query)
    {
        return mbeanServer.queryNames(name, query);
    }

    @Override
    public boolean isRegistered(ObjectName name)
    {
        return mbeanServer.isRegistered(name);
    }

    @Override
    public Integer getMBeanCount()
    {
        return mbeanServer.getMBeanCount();
    }

    @Override
    public Object getAttribute(ObjectName name, String attribute)
            throws MBeanException, AttributeNotFoundException, InstanceNotFoundException, ReflectionException
    {
        return mbeanServer.getAttribute(name, attribute);
    }

    @Override
    public AttributeList getAttributes(ObjectName name, String[] attributes)
            throws InstanceNotFoundException, ReflectionException
    {
        return mbeanServer.getAttributes(name, attributes);
    }

    @Override
    public void setAttribute(ObjectName name, Attribute attribute)
            throws InstanceNotFoundException, AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException
    {
        mbeanServer.setAttribute(name, attribute);
    }

    @Override
    public AttributeList setAttributes(ObjectName name, AttributeList attributes)
            throws InstanceNotFoundException, ReflectionException
    {
        return mbeanServer.setAttributes(name, attributes);
    }

    @Override
    public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
            throws InstanceNotFoundException, MBeanException, ReflectionException
    {
        return mbeanServer.invoke(name, operationName, params, signature);
    }

    @Override
    public String getDefaultDomain()
    {
        return mbeanServer.getDefaultDomain();
    }

    @Override
    public String[] getDomains()
    {
        return mbeanServer.getDomains();
    }

    @Override
    public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object context)
            throws InstanceNotFoundException
    {
        mbeanServer.addNotificationListener(name, listener, filter, context);
    }

    @Override
    public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object context)
            throws InstanceNotFoundException
    {
        mbeanServer.addNotificationListener(name, listener, filter, context);
    }

    @Override
    public void removeNotificationListener(ObjectName name, ObjectName listener)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        mbeanServer.removeNotificationListener(name, listener);
    }

    @Override
    public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object context)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        mbeanServer.removeNotificationListener(name, listener, filter, context);
    }

    @Override
    public void removeNotificationListener(ObjectName name, NotificationListener listener)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        mbeanServer.removeNotificationListener(name, listener);
    }

    @Override
    public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object context)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        mbeanServer.removeNotificationListener(name, listener, filter, context);
    }

    @Override
    public MBeanInfo getMBeanInfo(ObjectName name)
            throws InstanceNotFoundException, IntrospectionException, ReflectionException
    {
        return mbeanServer.getMBeanInfo(name);
    }

    @Override
    public boolean isInstanceOf(ObjectName name, String className)
            throws InstanceNotFoundException
    {
        return mbeanServer.isInstanceOf(name, className);
    }

    @Override
    public Object instantiate(String className)
            throws ReflectionException, MBeanException
    {
        return mbeanServer.instantiate(className);
    }

    @Override
    public Object instantiate(String className, ObjectName loaderName)
            throws ReflectionException, MBeanException, InstanceNotFoundException
    {
        return mbeanServer.instantiate(className, loaderName);
    }

    @Override
    public Object instantiate(String className, Object[] params, String[] signature)
            throws ReflectionException, MBeanException
    {
        return mbeanServer.instantiate(className, params, signature);
    }

    @Override
    public Object instantiate(String className, ObjectName loaderName, Object[] params, String[] signature)
            throws ReflectionException, MBeanException, InstanceNotFoundException
    {
        return mbeanServer.instantiate(className, loaderName, params, signature);
    }

    @SuppressWarnings("deprecation")
    @Override
    @Deprecated
    public ObjectInputStream deserialize(ObjectName name, byte[] data)
            throws OperationsException
    {
        return mbeanServer.deserialize(name, data);
    }

    @SuppressWarnings("deprecation")
    @Override
    @Deprecated
    public ObjectInputStream deserialize(String className, byte[] data)
            throws OperationsException, ReflectionException
    {
        return mbeanServer.deserialize(className, data);
    }

    @SuppressWarnings("deprecation")
    @Override
    @Deprecated
    public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data)
            throws OperationsException, ReflectionException
    {
        return mbeanServer.deserialize(className, loaderName, data);
    }

    @Override
    public ClassLoader getClassLoaderFor(ObjectName mbeanName)
            throws InstanceNotFoundException
    {
        return mbeanServer.getClassLoaderFor(mbeanName);
    }

    @Override
    public ClassLoader getClassLoader(ObjectName loaderName)
            throws InstanceNotFoundException
    {
        return mbeanServer.getClassLoader(loaderName);
    }

    @Override
    public ClassLoaderRepository getClassLoaderRepository()
    {
        return mbeanServer.getClassLoaderRepository();
    }

    @Override
    public ObjectInstance createMBean(String className, ObjectName name)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanException, NotCompliantMBeanException
    {
        return mbeanServer.createMBean(className, name);
    }

    @Override
    public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanException, NotCompliantMBeanException, InstanceNotFoundException
    {
        return mbeanServer.createMBean(className, name, loaderName);
    }

    @Override
    public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanException, NotCompliantMBeanException
    {
        return mbeanServer.createMBean(className, name, params, signature);
    }

    @Override
    public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params, String[] signature)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanException, NotCompliantMBeanException, InstanceNotFoundException
    {
        return mbeanServer.createMBean(className, name, loaderName, params, signature);
    }
}
