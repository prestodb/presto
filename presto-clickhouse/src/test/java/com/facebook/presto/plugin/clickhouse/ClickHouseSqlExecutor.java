package com.facebook.presto.plugin.clickhouse;

import static java.util.Objects.requireNonNull;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.sql.SqlExecutor;

public class ClickHouseSqlExecutor
        implements SqlExecutor
{
    private final QueryRunner queryRunner;
    private final Session session;

    public ClickHouseSqlExecutor(QueryRunner queryRunner)
    {
        this(queryRunner, queryRunner.getDefaultSession());
    }

    public ClickHouseSqlExecutor(QueryRunner queryRunner, Session session)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public void execute(String sql)
    {
        try {
            queryRunner.execute(session, sql);
        }
        catch (Throwable e) {
            throw new RuntimeException("Error executing sql:\n" + sql, e);
        }
    }
}
