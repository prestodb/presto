package com.facebook.presto.cli;

public final class Help
{
    private Help() {}

    public static String getHelpText()
    {
        return "" +
                "Supported commands:\n" +
                "QUIT\n" +
                "EXPLAIN [FORMAT {TEXT | GRAPHVIZ}] <query>\n" +
                "DESCRIBE <table>\n" +
                "SHOW COLUMNS FROM <table>\n" +
                "SHOW FUNCTIONS\n" +
                "SHOW SCHEMAS\n" +
                "SHOW PARTITIONS FROM <table> [WHERE ...] [ORDER BY ...] [LIMIT n]\n" +
                "SHOW TABLES [LIKE <pattern>]\n" +
                "";
    }
}
