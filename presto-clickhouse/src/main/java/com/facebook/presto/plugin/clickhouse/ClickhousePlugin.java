package com.facebook.presto.plugin.clickhouse;
/*
 * ----------------------------------------------------------------------
 * Copyright © 2014-2021 China Mobile (SuZhou) Software Technology Co.,Ltd.
 *
 * The programs can not be copied and/or distributed without the express
 * permission of China Mobile (SuZhou) Software Technology Co.,Ltd.
 *
 * ----------------------------------
 */

import com.facebook.presto.plugin.jdbc.JdbcPlugin;

/**
 * @author ahern
 * @date 2021/8/31 15:50
 * @since 1.0
 */
public class ClickhousePlugin extends JdbcPlugin {

    private static final String PLUGIN_NAME="clickhouse";

    public ClickhousePlugin() {
        super(PLUGIN_NAME, new ClickhouseClientModule());
    }
}
