package com.facebook.presto;

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchMetadata;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.airlift.log.Logging;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Locale;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDoubleSplit
{
    public static final int SPLITS_PER_NODE = 2;

    public static final ConnectorSession SESSION = new ConnectorSession("user", "source", "tpch", TpchMetadata.TINY_SCHEMA_NAME, UTC_KEY, Locale.ENGLISH, null, null);

    private final Closer closer = Closer.create();

    private LocalQueryRunner queryRunner;

    @BeforeMethod
    public void spinUp()
            throws Exception
    {
        Logging.initialize();

        this.queryRunner = closer.register(new LocalQueryRunner(SESSION));
        // add tpch
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(queryRunner.getNodeManager(), SPLITS_PER_NODE), ImmutableMap.<String, String>of());
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        closer.close();
    }

    @Test
    public void testTableExists()
            throws Exception
    {
        QualifiedTableName name = new QualifiedTableName("tpch", TpchMetadata.TINY_SCHEMA_NAME, "nation");
        Optional<TableHandle> handle = queryRunner.getMetadata().getTableHandle(SESSION, name);
        assertTrue(handle.isPresent());
    }

    @Test
    public void testTableHasData()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("SELECT count(1) from nation");

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(25)
                .build();

        assertEquals(result, expected);
    }
}
