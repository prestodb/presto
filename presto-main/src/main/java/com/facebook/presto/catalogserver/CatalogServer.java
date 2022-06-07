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
package com.facebook.presto.catalogserver;

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@ThriftService(value = "presto-catalog-server", idlName = "PrestoCatalogServer")
public class CatalogServer
{
    private final String EMPTY_STRING = "";
    private final Metadata metadataProvider;
    private final SessionPropertyManager sessionPropertyManager;
    private final TransactionManager transactionManager;
    private final ObjectMapper objectMapper;

    @Inject
    public CatalogServer(MetadataManager metadataProvider, SessionPropertyManager sessionPropertyManager, TransactionManager transactionManager, ObjectMapper objectMapper)
    {
        this.metadataProvider = requireNonNull(metadataProvider, "metadataProvider is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.objectMapper = requireNonNull(objectMapper, "handleResolver is null");
    }

    @ThriftMethod
    public boolean catalogExists(TransactionInfo transactionInfo, SessionRepresentation session, String catalogName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        return metadataProvider.catalogExists(session.toSession(sessionPropertyManager), catalogName);
    }

    @ThriftMethod
    public boolean schemaExists(TransactionInfo transactionInfo, SessionRepresentation session, CatalogSchemaName schema)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        return metadataProvider.schemaExists(session.toSession(sessionPropertyManager), schema);
    }

    @ThriftMethod
    public String getTableHandle(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName table)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        Optional<TableHandle> tableHandle = metadataProvider.getTableHandle(session.toSession(sessionPropertyManager), table);
        if (tableHandle.isPresent()) {
            try {
                return objectMapper.writeValueAsString(tableHandle.get());
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return EMPTY_STRING;
    }
}
