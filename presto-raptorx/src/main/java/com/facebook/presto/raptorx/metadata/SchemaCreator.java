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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.PrestoException;
import org.jdbi.v3.core.ConnectionFactory;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static java.util.Objects.requireNonNull;

public class SchemaCreator
{
    private final Database database;

    @Inject
    public SchemaCreator(Database database)
    {
        this.database = requireNonNull(database, "database is null");
    }

    @PostConstruct
    public void create()
    {
        createSchemaJdbi(database.getType(), database.getMasterConnection()).useHandle(handle -> {
            MasterSchemaDao dao = handle.attach(MasterSchemaDao.class);

            dao.createSequences();
            dao.createCurrentCommit();
            dao.createActiveCommit();
            dao.createCommits();
            dao.createSchemata();
            dao.createTables();
            dao.createColumns();
            dao.createViews();
            dao.createNodes();
            dao.createDistributions();
            dao.createBucketNodes();
            dao.createTransactions();
            dao.createTransactionTables();
            dao.createDeletedChunks();

            insertIgnore(database.getType(), handle, "current_commit (singleton, commit_id) VALUES ('X', 0)");
        });

        for (Database.Shard shard : database.getShards()) {
            createSchemaJdbi(database.getType(), shard.getConnection()).useHandle(handle -> {
                ShardSchemaDao dao = handle.attach(ShardSchemaDao.class);

                dao.createAbortedCommit();
                dao.createChunks();
                dao.createTableSizes();
                dao.createCreatedChunks();

                insertIgnore(database.getType(), handle, "aborted_commit (singleton, commit_id) VALUES ('X', 0)");
            });
        }
    }

    private static void insertIgnore(Database.Type type, Handle handle, String insert)
    {
        switch (type) {
            case H2:
            case MYSQL:
                handle.execute("INSERT IGNORE INTO " + insert);
                break;
            case POSTGRESQL:
                handle.execute("INSERT INTO " + insert + " ON CONFLICT DO NOTHING");
                break;
            default:
                throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Unhandled database: " + type);
        }
    }

    private static Jdbi createSchemaJdbi(Database.Type type, ConnectionFactory factory)
    {
        Jdbi jdbi = createJdbi(factory);
        if (type == Database.Type.POSTGRESQL) {
            jdbi.setTemplateEngine((template, context) -> template
                    .replace("BIGINT PRIMARY KEY AUTO_INCREMENT", "SERIAL")
                    .replace("LONGBLOB", "BYTEA")
                    .replace("MEDIUMBLOB", "BYTEA")
                    .replace("BLOB", "BYTEA")
                    .replaceAll("VARBINARY\\(\\d+\\)", "BYTEA"));
        }
        return jdbi;
    }
}
