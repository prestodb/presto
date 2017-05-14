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
package com.facebook.presto;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;

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
import javax.management.MalformedObjectNameException;
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
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NamespacedMBeanServer implements MBeanServer
{
    private final MBeanServer parent;
    private final String namespace;
    private static final Pattern BAD_PACKAGENAME_PATTERN = Pattern.compile("[//:?*]");
    private static final String SEPARATOR = ";";
    private final Map<ObjectName, ObjectName> nameRegitration;
    private final Map<ObjectName, ObjectName> reverseTranslation;

    public NamespacedMBeanServer(String namespace, MBeanServer parent)
    {
        requireNonNull(parent,  "parent is null");
        requireNonNull(namespace, "namespace is null");
        checkArgument(!BAD_PACKAGENAME_PATTERN.matcher(namespace).find(), "namespace cannot contain //:?* characters");
        this.parent = parent;
        this.namespace = namespace;
        this.nameRegitration = new MapMaker().weakValues().makeMap();
        this.reverseTranslation = new MapMaker().weakValues().makeMap();
    }

    public ObjectInstance createMBean(String className, ObjectName name)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException
    {
        return createMBean(className, name, (Object[]) null, (String[]) null);
    }

    public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException, InstanceNotFoundException
    {
        return createMBean(className, name, loaderName, (Object[]) null,
                (String[]) null);
    }

    public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException
    {
        try {
            return createMBean(className, name, null, params, signature);
        }
        catch (InstanceNotFoundException e)  {
            /* Can only happen if loaderName doesn't exist, but we just
               passed null, so we shouldn't get this exception.  */
            throw initCause(
                    new IllegalArgumentException("Unexpected exception: " + e), e);
        }
    }

    public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params, String[] signature)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException, InstanceNotFoundException
    {
        try {
            name = getNamespacedObjectName(name);
            return getUnnamespacedObjectInstance(parent.createMBean(className, name, loaderName, params, signature));
        }
        catch (MalformedObjectNameException e) {
            throw new MBeanRegistrationException(e);
        }
    }

    public ObjectInstance registerMBean(Object object, ObjectName name)
            throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException
    {
        try {
            name = getNamespacedObjectName(name);
            return getUnnamespacedObjectInstance(parent.registerMBean(object, name));
        }
        catch (MalformedObjectNameException e) {
            throw new MBeanRegistrationException(e);
        }
    }

    public void unregisterMBean(ObjectName name)
            throws InstanceNotFoundException, MBeanRegistrationException
    {
        try {
            name = getNamespacedObjectName(name);
        }
        catch (MalformedObjectNameException e) {
            throw new MBeanRegistrationException(e);
        }
        parent.unregisterMBean(name);
    }

    public ObjectInstance getObjectInstance(ObjectName name)
            throws InstanceNotFoundException
    {
        try {
            name = getNamespacedObjectName(name);
            return getUnnamespacedObjectInstance(parent.getObjectInstance(name));
        }
        catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query)
    {
        //TODO  to implement, probably a visitor to extract filter namespace?
        try {
            name = getNamespacedObjectName(name);
            return parent.queryMBeans(name, null)
                    .stream().map(instance -> getUnnamespacedObjectInstance(instance))
                    .filter(instance -> {
                       try {
                           return query.apply(instance.getObjectName());
                       }
                       catch (Exception e) {
                           return false;
                       }
                    })
                    .collect(Collectors.toCollection(ImmutableSet::of));
        }
        catch (MalformedObjectNameException e) {
            return ImmutableSet.of();
        }
    }

    public Set<ObjectName> queryNames(ObjectName name, QueryExp query)
    {
        //TODO  to implement, probably a visitor to extract filter namespace?
        try {
            name = getNamespacedObjectName(name);
            return parent.queryNames(name, null)
                    .stream().map(objectName -> getUnnamespacedObjectName(objectName))
                    .filter(objectName -> {
                        try {
                            return query.apply(objectName);
                        }
                        catch (Exception e) {
                            return false;
                        }
                    })
                    .collect(Collectors.toCollection(ImmutableSet::of));
        }
        catch (MalformedObjectNameException e) {
            return ImmutableSet.of();
        }
    }

    public boolean isRegistered(ObjectName name)
    {
        try {
            name = getNamespacedObjectName(name);
            return parent.isRegistered(name);
        }
        catch (MalformedObjectNameException e) {
            return false;
        }
    }

    public Integer getMBeanCount()
    {
        return null;
    }

    public Object getAttribute(ObjectName name, String attribute)
            throws MBeanException, AttributeNotFoundException, InstanceNotFoundException, ReflectionException
    {
        try {
            name = getNamespacedObjectName(name);
            return parent.getAttribute(name, attribute);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public AttributeList getAttributes(ObjectName name, String[] attributes)
            throws InstanceNotFoundException, ReflectionException
    {
        try {
            name = getNamespacedObjectName(name);
            return parent.getAttributes(name, attributes);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void setAttribute(ObjectName name, Attribute attribute)
            throws InstanceNotFoundException, AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException
    {
        try {
            name = getNamespacedObjectName(name);
            parent.setAttribute(name, attribute);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public AttributeList setAttributes(ObjectName name, AttributeList attributes)
            throws InstanceNotFoundException, ReflectionException
    {
        try {
            name = getNamespacedObjectName(name);
            return parent.setAttributes(name, attributes);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
            throws InstanceNotFoundException, MBeanException, ReflectionException
    {
        try {
            name = getNamespacedObjectName(name);
            return parent.invoke(name, operationName, params, signature);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public String getDefaultDomain()
    {
        return parent.getDefaultDomain();
    }

    public String[] getDomains()
    {
        return parent.getDomains();
    }

    public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback)
            throws InstanceNotFoundException
    {
        try {
            name = getNamespacedObjectName(name);
            parent.addNotificationListener(name, listener, filter, handback);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback)
            throws InstanceNotFoundException
    {
        try {
            name = getNamespacedObjectName(name);
            listener = getNamespacedObjectName(listener);
            parent.addNotificationListener(name, listener, filter, handback);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void removeNotificationListener(ObjectName name, ObjectName listener)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        try {
            name = getNamespacedObjectName(name);
            listener = getNamespacedObjectName(listener);
            parent.removeNotificationListener(name, listener);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        try {
            name = getNamespacedObjectName(name);
            listener = getNamespacedObjectName(listener);
            parent.removeNotificationListener(name, listener);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void removeNotificationListener(ObjectName name, NotificationListener listener)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        try {
            name = getNamespacedObjectName(name);
            parent.removeNotificationListener(name, listener);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        try {
            name = getNamespacedObjectName(name);
            parent.removeNotificationListener(name, listener, filter, handback);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public MBeanInfo getMBeanInfo(ObjectName name)
            throws InstanceNotFoundException, IntrospectionException, ReflectionException
    {
        try {
            name = getNamespacedObjectName(name);
            return parent.getMBeanInfo(name);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public boolean isInstanceOf(ObjectName name, String className)
            throws InstanceNotFoundException
    {
        try {
            name = getNamespacedObjectName(name);
            return parent.isInstanceOf(name, className);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public Object instantiate(String className)
            throws ReflectionException, MBeanException
    {
        return parent.instantiate(className);
    }

    public Object instantiate(String className, ObjectName loaderName)
            throws ReflectionException, MBeanException, InstanceNotFoundException
    {
        return parent.instantiate(className, loaderName);
    }

    public Object instantiate(String className, Object[] params, String[] signature)
            throws ReflectionException, MBeanException
    {
        return parent.instantiate(className, params, signature);
    }

    public Object instantiate(String className, ObjectName loaderName, Object[] params, String[] signature)
            throws ReflectionException, MBeanException, InstanceNotFoundException
    {
        return parent.instantiate(className, loaderName, params, signature);
    }

    public ObjectInputStream deserialize(ObjectName name, byte[] data)
            throws InstanceNotFoundException, OperationsException
    {
        try {
            name = getNamespacedObjectName(name);
            return parent.deserialize(name, data);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public ObjectInputStream deserialize(String className, byte[] data)
            throws OperationsException, ReflectionException
    {
        return parent.deserialize(className, data);
    }

    public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data)
            throws InstanceNotFoundException, OperationsException, ReflectionException
    {
        return parent.deserialize(className, loaderName, data);
    }

    public ClassLoader getClassLoaderFor(ObjectName mbeanName)
            throws InstanceNotFoundException
    {
        try {
            mbeanName = getNamespacedObjectName(mbeanName);
            return parent.getClassLoaderFor(mbeanName);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public ClassLoader getClassLoader(ObjectName loaderName)
            throws InstanceNotFoundException
    {
        return parent.getClassLoaderFor(loaderName);
    }

    public ClassLoaderRepository getClassLoaderRepository()
    {
        return parent.getClassLoaderRepository();
    }

    private  ObjectName getNamespacedObjectName(ObjectName objectName)
            throws MalformedObjectNameException
    {
        ObjectName namespacedObjectName = nameRegitration.get(objectName);
        if (namespacedObjectName == null) {
            namespacedObjectName = new ObjectName(namespace + SEPARATOR + objectName.getCanonicalName());
            nameRegitration.put(objectName, namespacedObjectName);
            reverseTranslation.put(namespacedObjectName, objectName);
        }
        return namespacedObjectName;
    }

    private ObjectName getUnnamespacedObjectName(ObjectName objectName)
    {
        return reverseTranslation.get(objectName);
    }

    private ObjectInstance getUnnamespacedObjectInstance(ObjectInstance instance)
    {
        ObjectName name = getUnnamespacedObjectName(instance.getObjectName());
        String className = instance.getClassName();
        return new ObjectInstance(name, className);
    }

    private static <T extends Throwable> T initCause(T throwable,
            Throwable cause)
    {
        throwable.initCause(cause);
        return throwable;
    }
}
