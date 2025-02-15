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
package org.apache.parquet.anonymization;

import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

public class AnonymizationManagerFactory
{
    private AnonymizationManagerFactory() {}

    public static Optional<AnonymizationManager> getAnonymizationManager(String managerClassName,
                                                                         Configuration conf,
                                                                         String tableName)
            throws PrestoException
    {
        try {
            managerClassName = managerClassName == null ? "" : managerClassName.trim();

            if (managerClassName.isEmpty()) {
                throw new IllegalStateException("Anonymization manager class not specified");
            }

            Class<?> clazz = Class.forName(managerClassName);

            // Ensure the class implements AnonymizationManager
            if (!AnonymizationManager.class.isAssignableFrom(clazz)) {
                throw new IllegalStateException("Invalid anonymization manager class: " + managerClassName);
            }

            @SuppressWarnings("unchecked")
            Class<? extends AnonymizationManager> managerClass =
                    (Class<? extends AnonymizationManager>) clazz;

            // Instantiate using the static create method
            Method createMethod = managerClass.getMethod("create", Configuration.class, String.class);
            AnonymizationManager manager = (AnonymizationManager) createMethod.invoke(null, conf, tableName);

            return Optional.of(manager);
        }
        catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            throw new IllegalStateException("Failed to create anonymization manager: " + targetException.getMessage(), targetException);
        }
        catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Anonymization manager class not found: " + e.getMessage(), e);
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to create anonymization manager: " + e.getMessage(), e);
        }
    }
}
