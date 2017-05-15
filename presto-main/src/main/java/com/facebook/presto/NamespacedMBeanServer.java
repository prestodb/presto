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
    private final MBeanServer outer;
    private final String namespace;
    private static final Pattern BAD_PACKAGENAME_PATTERN = Pattern.compile("[//:?*]");
    private static final String SEPARATOR = ";";
    private final Map<ObjectName, ObjectName> nameInOuterNamespaceOf;
    private final Map<ObjectName, ObjectName> nameInCurrentNamespaceOf;

    public NamespacedMBeanServer(String namespace, MBeanServer outer)
    {
        requireNonNull(outer,  "outer is null");
        requireNonNull(namespace, "namespace is null");
        checkArgument(!BAD_PACKAGENAME_PATTERN.matcher(namespace).find(), "namespace cannot contain //:?* characters");
        this.outer = outer;
        this.namespace = namespace;
        this.nameInOuterNamespaceOf = new MapMaker().weakValues().makeMap();
        this.nameInCurrentNamespaceOf = new MapMaker().weakValues().makeMap();
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
            name = getObjectNameInOuterNamespace(name);
            return getObjectInstanceInCurrentNamespace(outer.createMBean(className, name, loaderName, params, signature));
        }
        catch (MalformedObjectNameException e) {
            throw new MBeanRegistrationException(e);
        }
    }

    public ObjectInstance registerMBean(Object object, ObjectName name)
            throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            return getObjectInstanceInCurrentNamespace(outer.registerMBean(object, name));
        }
        catch (MalformedObjectNameException e) {
            throw new MBeanRegistrationException(e);
        }
    }

    public void unregisterMBean(ObjectName name)
            throws InstanceNotFoundException, MBeanRegistrationException
    {
        try {
            ObjectName outerName = getObjectNameInOuterNamespace(name);
            outer.unregisterMBean(outerName);
            nameInOuterNamespaceOf.remove(name);
            nameInCurrentNamespaceOf.remove(outerName);
        }
        catch (MalformedObjectNameException e) {
            throw new MBeanRegistrationException(e);
        }
    }

    public ObjectInstance getObjectInstance(ObjectName name)
            throws InstanceNotFoundException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            return getObjectInstanceInCurrentNamespace(outer.getObjectInstance(name));
        }
        catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query)
    {
        //TODO  to implement, probably a visitor to extract filter namespace?
        try {
            name = getObjectNameInOuterNamespace(name);
            return outer.queryMBeans(name, null)
                    .stream().map(instance -> getObjectInstanceInCurrentNamespace(instance))
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
            name = getObjectNameInOuterNamespace(name);
            return outer.queryNames(name, null)
                    .stream().map(objectName -> getObjectNameInCurentNamespace(objectName))
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
            name = getObjectNameInOuterNamespace(name);
            return outer.isRegistered(name);
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
            name = getObjectNameInOuterNamespace(name);
            return outer.getAttribute(name, attribute);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public AttributeList getAttributes(ObjectName name, String[] attributes)
            throws InstanceNotFoundException, ReflectionException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            return outer.getAttributes(name, attributes);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void setAttribute(ObjectName name, Attribute attribute)
            throws InstanceNotFoundException, AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            outer.setAttribute(name, attribute);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public AttributeList setAttributes(ObjectName name, AttributeList attributes)
            throws InstanceNotFoundException, ReflectionException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            return outer.setAttributes(name, attributes);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
            throws InstanceNotFoundException, MBeanException, ReflectionException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            return outer.invoke(name, operationName, params, signature);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public String getDefaultDomain()
    {
        return outer.getDefaultDomain();
    }

    public String[] getDomains()
    {
        return outer.getDomains();
    }

    public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback)
            throws InstanceNotFoundException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            outer.addNotificationListener(name, listener, filter, handback);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback)
            throws InstanceNotFoundException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            listener = getObjectNameInOuterNamespace(listener);
            outer.addNotificationListener(name, listener, filter, handback);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void removeNotificationListener(ObjectName name, ObjectName listener)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            listener = getObjectNameInOuterNamespace(listener);
            outer.removeNotificationListener(name, listener);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            listener = getObjectNameInOuterNamespace(listener);
            outer.removeNotificationListener(name, listener);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void removeNotificationListener(ObjectName name, NotificationListener listener)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            outer.removeNotificationListener(name, listener);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback)
            throws InstanceNotFoundException, ListenerNotFoundException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            outer.removeNotificationListener(name, listener, filter, handback);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public MBeanInfo getMBeanInfo(ObjectName name)
            throws InstanceNotFoundException, IntrospectionException, ReflectionException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            return outer.getMBeanInfo(name);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public boolean isInstanceOf(ObjectName name, String className)
            throws InstanceNotFoundException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            return outer.isInstanceOf(name, className);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public Object instantiate(String className)
            throws ReflectionException, MBeanException
    {
        return outer.instantiate(className);
    }

    public Object instantiate(String className, ObjectName loaderName)
            throws ReflectionException, MBeanException, InstanceNotFoundException
    {
        return outer.instantiate(className, loaderName);
    }

    public Object instantiate(String className, Object[] params, String[] signature)
            throws ReflectionException, MBeanException
    {
        return outer.instantiate(className, params, signature);
    }

    public Object instantiate(String className, ObjectName loaderName, Object[] params, String[] signature)
            throws ReflectionException, MBeanException, InstanceNotFoundException
    {
        return outer.instantiate(className, loaderName, params, signature);
    }

    public ObjectInputStream deserialize(ObjectName name, byte[] data)
            throws InstanceNotFoundException, OperationsException
    {
        try {
            name = getObjectNameInOuterNamespace(name);
            return outer.deserialize(name, data);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public ObjectInputStream deserialize(String className, byte[] data)
            throws OperationsException, ReflectionException
    {
        return outer.deserialize(className, data);
    }

    public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data)
            throws InstanceNotFoundException, OperationsException, ReflectionException
    {
        return outer.deserialize(className, loaderName, data);
    }

    public ClassLoader getClassLoaderFor(ObjectName mbeanName)
            throws InstanceNotFoundException
    {
        try {
            mbeanName = getObjectNameInOuterNamespace(mbeanName);
            return outer.getClassLoaderFor(mbeanName);
        }
        catch (MalformedObjectNameException e) {
            throw initCause(new InstanceNotFoundException(e.getMessage()), e);
        }
    }

    public ClassLoader getClassLoader(ObjectName loaderName)
            throws InstanceNotFoundException
    {
        return outer.getClassLoaderFor(loaderName);
    }

    public ClassLoaderRepository getClassLoaderRepository()
    {
        return outer.getClassLoaderRepository();
    }

    private  ObjectName getObjectNameInOuterNamespace(ObjectName objectName)
            throws MalformedObjectNameException
    {
        ObjectName outerObjectName = nameInOuterNamespaceOf.get(objectName);
        if (outerObjectName == null) {
            outerObjectName = new ObjectName(namespace + SEPARATOR + objectName.getCanonicalName());
            nameInOuterNamespaceOf.put(objectName, outerObjectName);
            nameInCurrentNamespaceOf.put(outerObjectName, objectName);
        }
        return outerObjectName;
    }

    private ObjectName getObjectNameInCurentNamespace(ObjectName objectName)
    {
        return nameInCurrentNamespaceOf.get(objectName);
    }

    private ObjectInstance getObjectInstanceInCurrentNamespace(ObjectInstance instance)
    {
        ObjectName name = getObjectNameInCurentNamespace(instance.getObjectName());
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
