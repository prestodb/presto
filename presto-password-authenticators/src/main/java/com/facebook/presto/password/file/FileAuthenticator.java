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
package com.facebook.presto.password.file;

import com.facebook.airlift.http.server.BasicPrincipal;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.PasswordAuthenticator;

import javax.inject.Inject;

import java.io.File;
import java.io.FileNotFoundException;
import java.security.Principal;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger log = Logger.get(FileAuthenticator.class);
    private final Supplier<PasswordStore> passwordStoreSupplier;

    @Inject
    public FileAuthenticator(FileConfig config) throws FileNotFoundException
    {
        File file = config.getPasswordFile();
        if (!file.exists()) {
            log.error("File %s does not exist", file.getAbsolutePath());
            throw new FileNotFoundException("File " + file.getAbsolutePath() + " does not exist");
        }
        int cacheMaxSize = config.getAuthTokenCacheMaxSize();

        passwordStoreSupplier = memoizeWithExpiration(
                () -> new PasswordStore(file, cacheMaxSize),
                config.getRefreshPeriod().toMillis(),
                MILLISECONDS);
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        if (!passwordStoreSupplier.get().authenticate(user, password)) {
            throw new AccessDeniedException("Invalid credentials");
        }

        return new BasicPrincipal(user);
    }
}
