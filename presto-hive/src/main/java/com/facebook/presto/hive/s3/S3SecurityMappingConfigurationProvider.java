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
package com.facebook.presto.hive.s3;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.io.File;
import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ACCESS_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_IAM_ROLE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SECRET_KEY;
import static com.facebook.presto.plugin.base.util.JsonUtils.parseJson;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class S3SecurityMappingConfigurationProvider
        implements DynamicConfigurationProvider
{
    private static final Logger log = Logger.get(S3SecurityMappingConfigurationProvider.class);

    private static final Set<String> SCHEMES = ImmutableSet.of("s3", "s3a", "s3n");

    private final Supplier<S3SecurityMappings> mappings;
    private final Optional<String> roleCredentialName;
    private final Optional<String> colonReplacement;

    @Inject
    public S3SecurityMappingConfigurationProvider(S3SecurityMappingConfig config)
    {
        this(getMappings(config), config.getRoleCredentialName(), config.getColonReplacement());
    }

    private static Supplier<S3SecurityMappings> getMappings(S3SecurityMappingConfig config)
    {
        File configFile = config.getConfigFile().orElseThrow(() -> new IllegalArgumentException("config file not set"));
        Supplier<S3SecurityMappings> supplier = () -> parseJson(configFile.toPath(), S3SecurityMappings.class);
        if (!config.getRefreshPeriod().isPresent()) {
            return Suppliers.memoize(supplier::get);
        }
        return Suppliers.memoizeWithExpiration(
                () -> {
                    log.info("Refreshing S3 security mapping configuration from %s", configFile);
                    return supplier.get();
                },
                config.getRefreshPeriod().get().toMillis(),
                MILLISECONDS);
    }

    public S3SecurityMappingConfigurationProvider(Supplier<S3SecurityMappings> mappings, Optional<String> roleCredentialName, Optional<String> colonReplacement)
    {
        this.mappings = requireNonNull(mappings, "mappings is null");
        this.roleCredentialName = requireNonNull(roleCredentialName, "roleCredentialName is null");
        this.colonReplacement = requireNonNull(colonReplacement, "colonReplacement is null");
    }

    @Override
    public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri)
    {
        if (!SCHEMES.contains(uri.getScheme())) {
            return;
        }

        S3SecurityMapping mapping = mappings.get().getMapping(context.getIdentity(), uri)
                .orElseThrow(() -> new AccessDeniedException("No matching S3 security mapping"));

        mapping.getCredentials().ifPresent(credentials -> {
            configuration.set(S3_ACCESS_KEY, credentials.getAWSAccessKeyId());
            configuration.set(S3_SECRET_KEY, credentials.getAWSSecretKey());
        });

        selectRole(mapping, context).ifPresent(role -> {
            configuration.set(S3_IAM_ROLE, role);
        });
    }

    private Optional<String> selectRole(S3SecurityMapping mapping, HdfsContext context)
    {
        Optional<String> optionalSelected = getRoleFromExtraCredential(context);

        if (!optionalSelected.isPresent()) {
            if (!mapping.getAllowedIamRoles().isEmpty() && !mapping.getIamRole().isPresent()) {
                throw new AccessDeniedException("No S3 role selected and mapping has no default role");
            }
            verify(mapping.getIamRole().isPresent() || mapping.getCredentials().isPresent(), "mapping must have role or credential");
            return mapping.getIamRole();
        }

        String selected = optionalSelected.get();

        // selected role must match default or be allowed
        if (!selected.equals(mapping.getIamRole().orElse(null)) &&
                !mapping.getAllowedIamRoles().contains(selected)) {
            throw new AccessDeniedException("Selected S3 role is not allowed: " + selected);
        }

        return optionalSelected;
    }

    private Optional<String> getRoleFromExtraCredential(HdfsContext context)
    {
        Optional<String> extraCredentialRole = roleCredentialName.map(name -> context.getIdentity().getExtraCredentials().get(name));

        if (colonReplacement.isPresent()) {
            return extraCredentialRole.map(role -> role.replace(colonReplacement.get(), ":"));
        }
        return extraCredentialRole;
    }
}
