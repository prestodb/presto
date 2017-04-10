-- database: presto; groups: window;
SELECT nationkey, min(nationkey) OVER (PARTITION BY regionkey ORDER BY nationkey ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS min FROM tpch.tiny.nation
