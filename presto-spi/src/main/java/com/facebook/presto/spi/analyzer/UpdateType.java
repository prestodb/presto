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
package com.facebook.presto.spi.analyzer;

import java.util.HashMap;
import java.util.Map;

public enum UpdateType {
    CREATE_TABLE("CREATE TABLE"),
    CREATE_VIEW("CREATE VIEW"),
    INSERT("INSERT"),
    CREATE_MATERIALIZED_VIEW("CREATE MATERIALIZED VIEW"),
    CREATE_FUNCTION("CREATE FUNCTION"),
    ADD_COLUMN("ADD COLUMN"),
    CREATE_SCHEMA("CREATE SCHEMA"),
    DROP_SCHEMA("DROP SCHEMA"),
    RENAME_SCHEMA("RENAME SCHEMA"),
    CREATE_ROLE("CREATE ROLE"),
    DELETE("DELETE"),
    ANALYZE("ANALYZE"),
    UPDATE("UPDATE"),
    DROP_ROLE("DROP ROLE"),
    SET_ROLE("SET ROLE"),
    GRANT_ROLE("GRANT ROLE"),
    REVOKE_ROLES("REVOKE ROLES"),
    CREATE_TYPE("CREATE TYPE"),
    TRUNCATE_TABLE("TRUNCATE TABLE"),
    DROP_TABLE("DROP TABLE"),
    RENAME_TABLE("RENAME TABLE"),
    RENAME_COLUMN("RENAME COLUMN"),
    SET_PROPERTIES("SET PROPERTIES"),
    DROP_COLUMN("DROP COLUMN"),
    DROP_CONSTRAINT("DROP CONSTRAINT"),
    ALTER_COLUMN_NOT_NULL("ALTER COLUMN NOT NULL"),
    RENAME_VIEW("RENAME VIEW"),
    DROP_VIEW("DROP VIEW"),
    DROP_MATERIALIZED_VIEW("DROP MATERIALIZED VIEW"),
    GRANT("GRANT"),
    REVOKE("REVOKE"),
    CALL("CALL"),
    ADD_CONSTRAINT("ADD CONSTRAINT"),
    UPDATE_TYPE("UPDATE TYPE"),
    SET_SESSION("SET SESSION"),
    USE("USE"),
    START_TRANSACTION("START TRANSACTION"),
    COMMIT("COMMIT"),
    RESET_SESSION("RESET SESSION"),
    ROLLBACK("ROLLBACK"),
    PREPARE("PREPARE"),
    DEALLOCATE("DEALLOCATE"),
    DROP_FUNCTION("DROP FUNCTION");

    private final String updateType;
    private static final Map<String, UpdateType> TYPE_MAP = new HashMap<>();

    static {
        for (UpdateType type : UpdateType.values()) {
            TYPE_MAP.put(type.getUpdateType(), type);
        }
    }

    UpdateType(String updateType)
    {
        this.updateType = updateType;
    }

    public String getUpdateType()
    {
        return updateType;
    }

    public static UpdateType getValue(String type)
    {
        return TYPE_MAP.get(type);
    }
}
