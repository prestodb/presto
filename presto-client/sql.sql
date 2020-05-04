SELECT CAST("date"("from_unixtime"(CAST(c.time_created AS bigint))) AS varchar) ds
     , "from_unixtime"(CAST(c.time_created AS bigint))                          action_time
     , 'L0'                                                                     queue_name_to
     , 'Inflow'                                                                 metric_name
     , CAST("json_extract"(pc.job_data, '$.screening_type') AS varchar)         screening_type
     , 'PCR'                                                                    system
     , c.paycom_status                                                          status_to
     , CAST(pj.id1 AS varchar)                                                  metric_value
     , 'CSI'                                                                    vendor
FROM (((("srt_job_pay_com_20323:bizapps" c
    INNER JOIN srt_job_pay_com_20323_max sjpc_max ON (1 = 1))
    INNER JOIN srt_job_pay_com_to_parent_job sjpct_max ON (1 = 1))
    INNER JOIN "srt_job_pay_com_to_parent_job:bizapps" pj ON ((CAST(c.fbid AS bigint) = pj.id1) AND (c.ds = pj.ds)))
         INNER JOIN "srt_job_pay_com_20323:bizapps" pc ON ((pj.id2 = CAST(pc.fbid AS bigint)) AND (pj.ds = pc.ds)))
WHERE ((((((c.ds = sjpc_max.max_ds) AND (pj.ds = sjpct_max.max_ds)) AND (pc.ds = sjpc_max.max_ds)) AND (c.queue_id = '830457350656188')) AND
        (c.vertical_id = '195238044438408')) AND ("from_unixtime"(CAST(c.time_created AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp)))
UNION ALL
SELECT CAST("date"(updated_time) AS varchar) ds
     , updated_time                          action_time
     , queue                                 queue_name_to
     , 'Inflow'                              metric_name
     , product                               screening_type
     , 'PCR'                                 system
     , paycom_status                         status_to
     , alert_id                              metric_value
     , 'CSI'                                 vendor
FROM (
         SELECT ds
              , ticket_id
              , alert_id
              , created
              , updated_time
              , prev_queue
              , queue
              , paycom_status
              , product
         FROM (
                  SELECT a.ds
                       , a.ticket_id
                       , a.alert_id
                       , CAST(a.created AS timestamp)                                                     created
                       , "from_unixtime"(CAST(a.updated_time AS bigint))                                  updated_time
                       , a.prev_queue
                       , a.queue
                       , 'ESCALATED'                                                                      paycom_status
                       , "split"(CAST("json_extract"(b.job_data, '$.screening_type') AS varchar), '"')[1] product
                       , 'null'                                                                           reasons
                  FROM ((
                            SELECT srt_paycom_jobalert_id       alert_id
                                 , object_id                    ticket_id
                                 , srt_paycom_original_queue    prev_queue
                                 , srt_paycom_destination_queue queue
                                 , updated_time
                                 , created
                                 , ds
                            FROM (
                                     SELECT *
                                     FROM ("pcr_alert_queue_change_activity_23089:bizapps" act
                                              LEFT JOIN (
                                         SELECT DISTINCT fbid
                                                       , is_test_case
                                         FROM "srt_job_pay_com_20323:bizapps"
                                         WHERE (ds = (SELECT "max"(ds)
                                                      FROM "srt_job_pay_com_20323:bizapps"
                                         ))
                                     ) is_test ON (act.object_id = is_test.fbid))
                                     WHERE ((is_test.is_test_case <> '1') AND (((ds = (SELECT "max"(ds)
                                                                                       FROM "pcr_alert_queue_change_activity_23089:bizapps"
                                     )) AND ("lower"(srt_paycom_original_queue) IN ('l0', 'l1a', 'l1b', 'l2a'))) AND
                                                                               ("from_unixtime"(CAST(updated_time AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp))))
                                 )
                        ) a
                           INNER JOIN (
                      SELECT fbid
                           , paycom_status
                           , job_data
                      FROM "srt_job_pay_com_20323:bizapps"
                      WHERE ((((("lower"(escalation_queue) IN ('l0', 'l1a', 'l1b', 'l2a', 'l2b', 'l3')) AND ("lower"(is_test_case) <> '1')) AND
                               (tower = 'Sanction')) AND (search_enabled = '1')) AND (ds = (SELECT "max"(ds)
                                                                                            FROM "srt_job_pay_com_20323:bizapps"
                      )))
                  ) b ON (a.ticket_id = b.fbid))
              )
         UNION ALL
         SELECT ds
              , ticket_id
              , alert_id
              , created
              , updated_time
              , prev_queue
              , queue
              , paycom_status
              , product
         FROM (
                  SELECT a.ds
                       , a.ticket_id
                       , a.alert_id
                       , a.created
                       , a.updated_time
                       , 'null'                                                                           prev_queue
                       , b.queue
                       , a.paycom_status
                       , "split"(CAST("json_extract"(b.job_data, '$.screening_type') AS varchar), '"')[1] product
                       , b.reasons
                  FROM ((
                            SELECT DISTINCT object_id                                     ticket_id
                                          , srt_paycom_jobalert_id                        alert_id
                                          , CAST(created AS timestamp)                    created
                                          , "from_unixtime"(CAST(updated_time AS bigint)) updated_time
                                          , 'REOPENED'                                    paycom_status
                                          , ds
                            FROM "pcr_alert_reopen_activity_23434:bizapps"
                            WHERE ((ds = (SELECT "max"(ds)
                                          FROM "pcr_alert_reopen_activity_23434:bizapps"
                            )) AND ("from_unixtime"(CAST(updated_time AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp)))
                        ) a
                           INNER JOIN (
                      SELECT fbid
                           , paycom_status
                           , ds
                           , escalation_queue queue
                           , job_data
                           , reasons
                      FROM "srt_job_pay_com_20323:bizapps"
                      WHERE ((((("lower"(escalation_queue) IN ('l0', 'l1a', 'l1b', 'l2a', 'l2b', 'l3')) AND ("lower"(is_test_case) <> '1')) AND
                               (tower = 'Sanction')) AND (search_enabled = '1')) AND (ds = (SELECT "max"(ds)
                                                                                            FROM "srt_job_pay_com_20323:bizapps"
                      )))
                  ) b ON (a.ticket_id = b.fbid))
                  WHERE (b.paycom_status = 'REOPENED')
              )
     )
UNION ALL
SELECT CAST("date"(updated_time) AS varchar) ds
     , updated_time                          action_time
     , queue                                 queue_name_to
     , 'Outflow'                             metric_name
     , product                               screening_type
     , 'PCR'                                 system
     , paycom_status                         status_to
     , alert_id                              metric_value
     , 'CSI'                                 vendor
FROM (
         SELECT a.ds
              , a.ticket_id
              , a.alert_id
              , a.created
              , a.updated_time
              , 'null'                                                                           prev_queue
              , b.queue
              , 'RESOLVED'                                                                       paycom_status
              , "split"(CAST("json_extract"(b.job_data, '$.screening_type') AS varchar), '"')[1] product
              , b.reasons
         FROM ((
                   SELECT DISTINCT object_id                                     ticket_id
                                 , srt_paycom_jobalert_id                        alert_id
                                 , CAST(created AS timestamp)                    created
                                 , "from_unixtime"(CAST(updated_time AS bigint)) updated_time
                                 , ds
                   FROM "pcr_alert_resolve_activity_23411:bizapps"
                   WHERE ((ds = (SELECT "max"(ds)
                                 FROM "pcr_alert_resolve_activity_23411:bizapps"
                   )) AND ("from_unixtime"(CAST(updated_time AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp)))
               ) a
                  INNER JOIN (
             SELECT fbid
                  , paycom_status
                  , escalation_queue queue
                  , ds
                  , job_data
                  , reasons
             FROM "srt_job_pay_com_20323:bizapps"
             WHERE ((((("lower"(escalation_queue) IN ('l0', 'l1a', 'l1b', 'l2a', 'l2b', 'l3')) AND ("lower"(is_test_case) <> '1')) AND (tower = 'Sanction')) AND
                     (search_enabled = '1')) AND (ds = (SELECT "max"(ds)
                                                        FROM "srt_job_pay_com_20323:bizapps"
             )))
         ) b ON (a.ticket_id = b.fbid))
     ) as ab2
UNION ALL
SELECT CAST("date"(updated_time) AS varchar) ds
     , updated_time                          action_time
     , prev_queue                            queue_name_to
     , 'Outflow'                             metric_name
     , product                               screening_type
     , 'PCR'                                 system
     , paycom_status                         status_to
     , alert_id                              metric_value
     , 'CSI'                                 vendor
FROM (
         SELECT a.ds
              , a.ticket_id
              , a.alert_id
              , CAST(a.created AS timestamp)                                                     created
              , "from_unixtime"(CAST(a.updated_time AS bigint))                                  updated_time
              , a.prev_queue
              , a.queue
              , 'ESCALATED'                                                                      paycom_status
              , "split"(CAST("json_extract"(b.job_data, '$.screening_type') AS varchar), '"')[1] product
              , 'null'                                                                           reasons
         FROM ((
                   SELECT srt_paycom_jobalert_id       alert_id
                        , object_id                    ticket_id
                        , srt_paycom_original_queue    prev_queue
                        , srt_paycom_destination_queue queue
                        , updated_time
                        , created
                        , ds
                   FROM (
                            SELECT *
                            FROM ("pcr_alert_queue_change_activity_23089:bizapps" act
                                     LEFT JOIN (
                                SELECT DISTINCT fbid
                                              , is_test_case
                                FROM "srt_job_pay_com_20323:bizapps"
                                WHERE (ds = (SELECT "max"(ds)
                                             FROM "srt_job_pay_com_20323:bizapps"
                                ))
                            ) is_test ON (act.object_id = is_test.fbid))
                            WHERE ((is_test.is_test_case <> '1') AND (((ds = (SELECT "max"(ds)
                                                                              FROM "pcr_alert_queue_change_activity_23089:bizapps"
                            )) AND ("lower"(srt_paycom_original_queue) IN ('l0', 'l1a', 'l2a', 'l1b'))) AND
                                                                      ("from_unixtime"(CAST(updated_time AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp))))
                        ) as ait
               ) a
                  INNER JOIN (
             SELECT fbid
                  , paycom_status
                  , job_data
             FROM "srt_job_pay_com_20323:bizapps"
             WHERE ((((("lower"(escalation_queue) IN ('l0', 'l1a', 'l1b', 'l2a', 'l2b', 'l3')) AND ("lower"(is_test_case) <> '1')) AND (tower = 'Sanction')) AND
                     (search_enabled = '1')) AND (ds = (SELECT "max"(ds)
                                                        FROM "srt_job_pay_com_20323:bizapps"
             )))
         ) b ON (a.ticket_id = b.fbid))
     ) as ab
UNION ALL
SELECT CAST("date"(updated_time) AS varchar) ds
     , updated_time                          action_time
     , prev_queue                            queue_name_to
     , 'Outflow'                             metric_name
     , product                               screening_type
     , 'PCR'                                 system
     , paycom_status                         status_to
     , alert_id                              metric_value
     , 'CSI'                                 vendor
FROM (
         SELECT a.ds
              , a.ticket_id
              , a.alert_id
              , a.created
              , a.updated_time
              , 'null'                                                                           prev_queue
              , b.queue
              , 'WAIT FOR USER'                                                                  paycom_status
              , "split"(CAST("json_extract"(b.job_data, '$.screening_type') AS varchar), '"')[1] product
              , b.reasons
         FROM ((
                   SELECT DISTINCT object_id                                     ticket_id
                                 , srt_paycom_jobalert_id                        alert_id
                                 , CAST(created AS timestamp)                    created
                                 , "from_unixtime"(CAST(updated_time AS bigint)) updated_time
                                 , ds
                   FROM "pcr_alert_user_wait_activity_24615:bizapps"
                   WHERE ((ds = (SELECT "max"(ds)
                                 FROM "pcr_alert_user_wait_activity_24615:bizapps"
                   )) AND ("from_unixtime"(CAST(updated_time AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp)))
               ) a
                  INNER JOIN (
             SELECT fbid
                  , paycom_status
                  , escalation_queue queue
                  , ds
                  , job_data
                  , reasons
             FROM "srt_job_pay_com_20323:bizapps"
             WHERE ((((("lower"(escalation_queue) IN ('l0', 'l1a', 'l1b', 'l2a', 'l2b', 'l3')) AND ("lower"(is_test_case) <> '1')) AND (tower = 'Sanction')) AND
                     (search_enabled = '1')) AND (ds = (SELECT "max"(ds)
                                                        FROM "srt_job_pay_com_20323:bizapps"
             )))
         ) b ON (a.ticket_id = b.fbid))
     )
UNION ALL
SELECT CAST("date"(updated_time) AS varchar) ds
     , updated_time                          action_time
     , queue                                 queue_name_to
     , 'auto_close'                          metric_name
     , product                               screening_type
     , 'PCR'                                 system
     , paycom_status                         status_to
     , alert_id                              metric_value
     , 'CSI'                                 vendor
FROM ((
          SELECT a.ds
               , a.ticket_id
               , a.alert_id
               , a.created
               , a.updated_time
               , 'null'                                                                           prev_queue
               , b.queue
               , 'RESOLVED'                                                                       paycom_status
               , "split"(CAST("json_extract"(b.job_data, '$.screening_type') AS varchar), '"')[1] product
               , b.reasons
          FROM ((
                    SELECT DISTINCT object_id                                     ticket_id
                                  , srt_paycom_jobalert_id                        alert_id
                                  , CAST(created AS timestamp)                    created
                                  , "from_unixtime"(CAST(updated_time AS bigint)) updated_time
                                  , ds
                    FROM "pcr_alert_resolve_activity_23411:bizapps"
                    WHERE ((ds = (SELECT "max"(ds)
                                  FROM "pcr_alert_resolve_activity_23411:bizapps"
                    )) AND ("from_unixtime"(CAST(updated_time AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp)))
                ) a
                   INNER JOIN (
              SELECT fbid
                   , paycom_status
                   , escalation_queue queue
                   , ds
                   , job_data
                   , reasons
              FROM "srt_job_pay_com_20323:bizapps"
              WHERE ((((("lower"(escalation_queue) IN ('l0', 'l1a', 'l1b', 'l2a', 'l2b', 'l3')) AND ("lower"(is_test_case) <> '1')) AND (tower = 'Sanction')) AND
                      (search_enabled = '1')) AND (ds = (SELECT "max"(ds)
                                                         FROM "srt_job_pay_com_20323:bizapps"
              )))
          ) b ON (a.ticket_id = b.fbid))
      ) a
         INNER JOIN (
    SELECT pj.id1
         , c.reasons
    FROM ((("srt_job_pay_com_20323:bizapps" c
        INNER JOIN srt_job_pay_com_20323_max sjpc_max ON (1 = 1))
        INNER JOIN "srt_job_pay_com_to_parent_job:bizapps" pj ON ((CAST(c.fbid AS bigint) = pj.id1) AND (c.ds = pj.ds)))
             INNER JOIN "srt_job_pay_com_20323:bizapps" pc ON ((pj.id2 = CAST(pc.fbid AS bigint)) AND (pj.ds = pc.ds)))
    WHERE ((((((c.ds = sjpc_max.max_ds) AND (pj.ds = (SELECT "max"(ds)
                                                      FROM "srt_job_pay_com_to_parent_job:bizapps"
    ))) AND (pc.ds = (SELECT "max"(ds)
                      FROM "srt_job_pay_com_20323:bizapps"
    ))) AND (c.paycom_status = 'RESOLVED')) AND (c.vertical_id = '195238044438408')) AND
           ("from_unixtime"(CAST(c.time_created AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp)))
) b ON (CAST(a.alert_id AS bigint) = b.id1))
WHERE (("lower"(a.queue) <> 'test') AND (b.reasons LIKE '%AUTO_CLOSE_BY_RULE%'))
UNION ALL
SELECT CAST("date"(updated_time) AS varchar) ds
     , updated_time                          action_time
     , queue                                 queue_name_to
     , 'manual_close'                        metric_name
     , product                               screening_type
     , 'PCR'                                 system
     , paycom_status                         status_to
     , alert_id                              metric_value
     , 'CSI'                                 vendor
FROM ((
          SELECT *
          FROM (
                   SELECT a.ds
                        , a.ticket_id
                        , a.alert_id
                        , a.created
                        , a.updated_time
                        , 'null'                                                                           prev_queue
                        , b.queue
                        , 'RESOLVED'                                                                       paycom_status
                        , "split"(CAST("json_extract"(b.job_data, '$.screening_type') AS varchar), '"')[1] product
                        , b.reasons
                   FROM ((
                             SELECT DISTINCT object_id                                     ticket_id
                                           , srt_paycom_jobalert_id                        alert_id
                                           , CAST(created AS timestamp)                    created
                                           , "from_unixtime"(CAST(updated_time AS bigint)) updated_time
                                           , ds
                             FROM "pcr_alert_resolve_activity_23411:bizapps"
                             WHERE ((ds = (SELECT "max"(ds)
                                           FROM "pcr_alert_resolve_activity_23411:bizapps"
                             )) AND ("from_unixtime"(CAST(updated_time AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp)))
                         ) a
                            INNER JOIN (
                       SELECT fbid
                            , paycom_status
                            , escalation_queue queue
                            , ds
                            , job_data
                            , reasons
                       FROM "srt_job_pay_com_20323:bizapps"
                       WHERE ((((("lower"(escalation_queue) IN ('l0', 'l1a', 'l1b', 'l2a', 'l2b', 'l3')) AND ("lower"(is_test_case) <> '1')) AND
                                (tower = 'Sanction')) AND (search_enabled = '1')) AND (ds = (SELECT "max"(ds)
                                                                                             FROM "srt_job_pay_com_20323:bizapps"
                       )))
                   ) b ON (a.ticket_id = b.fbid))
               )
          UNION ALL
          SELECT *
          FROM (
                   SELECT a.ds
                        , a.ticket_id
                        , a.alert_id
                        , CAST(a.created AS timestamp)                                                     created
                        , "from_unixtime"(CAST(a.updated_time AS bigint))                                  updated_time
                        , a.prev_queue
                        , a.queue
                        , 'ESCALATED'                                                                      paycom_status
                        , "split"(CAST("json_extract"(b.job_data, '$.screening_type') AS varchar), '"')[1] product
                        , 'null'                                                                           reasons
                   FROM ((
                             SELECT srt_paycom_jobalert_id       alert_id
                                  , object_id                    ticket_id
                                  , srt_paycom_original_queue    prev_queue
                                  , srt_paycom_destination_queue queue
                                  , updated_time
                                  , created
                                  , ds
                             FROM (
                                      SELECT *
                                      FROM ("pcr_alert_queue_change_activity_23089:bizapps" act
                                               LEFT JOIN (
                                          SELECT DISTINCT fbid
                                                        , is_test_case
                                          FROM "srt_job_pay_com_20323:bizapps"
                                          WHERE (ds = (SELECT "max"(ds)
                                                       FROM "srt_job_pay_com_20323:bizapps"
                                          ))
                                      ) is_test ON (act.object_id = is_test.fbid))
                                      WHERE ((is_test.is_test_case <> '1') AND (((ds = (SELECT "max"(ds)
                                                                                        FROM "pcr_alert_queue_change_activity_23089:bizapps"
                                      )) AND ("lower"(srt_paycom_original_queue) IN ('l0', 'l1a', 'l2a', 'l1b'))) AND
                                                                                ("from_unixtime"(CAST(updated_time AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp))))
                                  )
                         ) a
                            INNER JOIN (
                       SELECT fbid
                            , paycom_status
                            , job_data
                       FROM "srt_job_pay_com_20323:bizapps"
                       WHERE ((((("lower"(escalation_queue) IN ('l0', 'l1a', 'l1b', 'l2a', 'l2b', 'l3')) AND ("lower"(is_test_case) <> '1')) AND
                                (tower = 'Sanction')) AND (search_enabled = '1')) AND (ds = (SELECT "max"(ds)
                                                                                             FROM "srt_job_pay_com_20323:bizapps"
                       )))
                   ) b ON (a.ticket_id = b.fbid))
               )
          UNION ALL
          SELECT *
          FROM (
                   SELECT a.ds
                        , a.ticket_id
                        , a.alert_id
                        , a.created
                        , a.updated_time
                        , 'null'                                                                           prev_queue
                        , b.queue
                        , 'WAIT FOR USER'                                                                  paycom_status
                        , "split"(CAST("json_extract"(b.job_data, '$.screening_type') AS varchar), '"')[1] product
                        , b.reasons
                   FROM ((
                             SELECT DISTINCT object_id                                     ticket_id
                                           , srt_paycom_jobalert_id                        alert_id
                                           , CAST(created AS timestamp)                    created
                                           , "from_unixtime"(CAST(updated_time AS bigint)) updated_time
                                           , ds
                             FROM "pcr_alert_user_wait_activity_24615:bizapps"
                             WHERE ((ds = (SELECT "max"(ds)
                                           FROM "pcr_alert_user_wait_activity_24615:bizapps"
                             )) AND ("from_unixtime"(CAST(updated_time AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp)))
                         ) a
                            INNER JOIN (
                       SELECT fbid
                            , paycom_status
                            , escalation_queue queue
                            , ds
                            , job_data
                            , reasons
                       FROM "srt_job_pay_com_20323:bizapps"
                       WHERE ((((("lower"(escalation_queue) IN ('l0', 'l1a', 'l1b', 'l2a', 'l2b', 'l3')) AND ("lower"(is_test_case) <> '1')) AND
                                (tower = 'Sanction')) AND (search_enabled = '1')) AND (ds = (SELECT "max"(ds)
                                                                                             FROM "srt_job_pay_com_20323:bizapps"
                       )))
                   ) b ON (a.ticket_id = b.fbid))
               )
      ) a
         INNER JOIN (
    SELECT pj.id1
         , c.reasons
    FROM ((("srt_job_pay_com_20323:bizapps" c
        INNER JOIN srt_job_pay_com_20323_max sjpc_max ON (1 = 1))
        INNER JOIN "srt_job_pay_com_to_parent_job:bizapps" pj ON ((CAST(c.fbid AS bigint) = pj.id1) AND (c.ds = pj.ds)))
             INNER JOIN "srt_job_pay_com_20323:bizapps" pc ON ((pj.id2 = CAST(pc.fbid AS bigint)) AND (pj.ds = pc.ds)))
    WHERE (((((c.ds = sjpc_max.max_ds) AND (pj.ds = (SELECT "max"(ds)
                                                     FROM "srt_job_pay_com_to_parent_job:bizapps"
    ))) AND (pc.ds = (SELECT "max"(ds)
                      FROM "srt_job_pay_com_20323:bizapps"
    ))) AND (c.vertical_id = '195238044438408')) AND ("from_unixtime"(CAST(c.time_created AS bigint)) >= CAST('2019-02-04 13:00:00' AS timestamp)))
) b ON (CAST(a.alert_id AS bigint) = b.id1))
WHERE (("lower"(a.queue) <> 'test') AND (NOT (b.reasons LIKE '%AUTO_CLOSE_BY_RULE%')))
UNION ALL
SELECT CAST("date"((action_time + INTERVAL '8' HOUR)) AS varchar) ds
     , (action_time + INTERVAL '8' HOUR)                          action_time
     , queue_name_to
     , metric_name
     , screening_type
     , system
     , status_to
     , metric_value
     , vendor
FROM (
         SELECT creation_time              ds
              , creation_time              action_time
              , 'L0'                       queue_name_to
              , 'Inflow'                   metric_name
              , screening_type
              , 'Leon'                     system
              , 'New'                      status_to
              , CAST(ticket_id AS varchar) metric_value
              , vendor
         FROM (paycom_dim_leon_tickets
                  INNER JOIN paycom_dim_leon_tickets_max pdlt_max ON (1 = 1))
         WHERE ((ds = pdlt_max.max_ds) AND ("lower"(latest_queue_name) = 'l0'))
         UNION ALL
         SELECT creation_time              ds
              , creation_time              action_time
              , 'L1a'                      queue_name_to
              , 'Inflow'                   metric_name
              , screening_type
              , 'Leon'                     system
              , 'NEW'                      status_to
              , CAST(ticket_id AS varchar) metric_value
              , vendor
         FROM (paycom_dim_leon_tickets
                  INNER JOIN paycom_dim_leon_tickets_max pdlt_max ON (1 = 1))
         WHERE ((ds = pdlt_max.max_ds) AND ("lower"(latest_queue_name) IN ('l1a', 'l1b', 'l2', 'escalated')))
         UNION ALL
         SELECT action_time                ds
              , action_time                action_time
              , queue_name_to_revised      queue_name_to
              , 'Inflow'                   metric_name
              , screening_type_enum_label  screening_type
              , 'Leon'                     system
              , status_to_revised          status_to
              , CAST(ticket_id AS varchar) metric_value
              , vendor                     vendor
         FROM (
                  SELECT b.ticket_id
                       , b.analyst_id
                       , b.analyst_name
                       , b.action_time_unix
                       , b.action_time
                       , b.transaction_data
                       , b.status_from
                       , b.status_to
                       , b.queue_name_from
                       , b.queue_name_to
                       , (CASE
                              WHEN (b.queue_name_to IS NULL)
                                  THEN "first_value"(b.queue_name_to) OVER (PARTITION BY b.ticket_id, b.grp_queue_to ORDER BY b.action_time ASC)
                              ELSE b.queue_name_from END)           queue_name_from_revised
                       , (CASE
                              WHEN (b.queue_name_to IS NULL) THEN "first_value"(b.queue_name_to)
                                                                                OVER (PARTITION BY b.ticket_id, b.grp_queue_from ORDER BY b.action_time ASC)
                              ELSE b.queue_name_to END)             queue_name_to_revised
                       , (CASE
                              WHEN (b.status_to IS NULL) THEN "first_value"(b.status_to) OVER (PARTITION BY b.ticket_id, b.grp_status ORDER BY b.action_time ASC)
                              ELSE b.status_to END)                 status_to_revised
                       , (CASE
                              WHEN (c.inquiry_origin = 59) THEN 'IG_SHOPPING'
                              WHEN (c.inquiry_origin = 60) THEN 'MACHAMP'
                              WHEN (c.inquiry_origin = 61) THEN 'WA_PAID_MESSAGES'
                              WHEN (c.inquiry_origin = 62) THEN 'IG_P4C'
                              WHEN (c.inquiry_origin = 63) THEN 'IG_P4C_BILLING'
                              WHEN (c.inquiry_origin = 64) THEN 'C2C_COMMERCE'
                              WHEN (c.inquiry_origin = 65) THEN 'GLOBAL_POLITICAL_AUTHORIZATION'
                              WHEN (c.inquiry_origin = 66) THEN 'WA_P2P'
                              WHEN (c.inquiry_origin = 67) THEN 'OCULUS_PORTAL_HARDWARE_STORE'
                              WHEN (c.inquiry_origin = 68) THEN 'IG_P4C_INTL'
                              WHEN (c.inquiry_origin = 69) THEN 'B2C_MESSENGER_COMMERCE'
                              WHEN (c.inquiry_origin = 70) THEN 'VIEWPOINTS'
                              WHEN (c.inquiry_origin = 71) THEN 'GEOBLOCKING'
                              WHEN (c.inquiry_origin = 72) THEN 'VENEZUELA'
                              WHEN (c.inquiry_origin = 73) THEN 'WA_P2M_BRAZIL'
                              ELSE d.screening_type_enum_label END) screening_type_enum_label
                       , c.vendor
                  FROM (((
                      SELECT *
                           , "sum"((CASE WHEN (queue_name_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)   grp_queue_to
                           , "sum"((CASE WHEN (queue_name_from IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC) grp_queue_from
                           , "sum"((CASE WHEN (status_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)       grp_status
                      FROM (
                               SELECT ticket_id
                                    , status_from
                                    , status_to
                                    , queue_id_from
                                    , (CASE
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'USER_WAIT')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_to = 'OPEN') AND (queue_name_to = 'L1a')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (queue_name_to = 'L0') THEN 'L0'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (QUEUE_ID_from = 1651263241863038) THEN 'L0'
                                           ELSE queue_name_from END) queue_name_from
                                    , queue_id_to
                                    , (CASE
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((status_from = 'NEW') AND (status_to = 'OPEN')) THEN 'L1a'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'USER_WAIT')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_to IS NULL)) THEN 'L1a'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL)) AND
                                                 ("lag"(status_from, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'OPEN'))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           ELSE queue_name_to END)   queue_name_to
                                    , analyst_id
                                    , analyst_name
                                    , action_time_unix
                                    , action_time
                                    , transaction_data
                               FROM (
                                        SELECT ticket_id
                                             , (CASE
                                                    WHEN (status_from IS NULL) THEN "lag"(status_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                                    ELSE status_from END) status_from
                                             , status_to
                                             , queue_id_from
                                             , queue_name_from
                                             , queue_id_to
                                             , queue_name_to
                                             , analyst_id
                                             , analyst_name
                                             , action_time_unix
                                             , action_time
                                             , transaction_data
                                        FROM (
                                                 SELECT ticket_id
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2006') THEN 'USER_WAIT' END) status_from
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2006') THEN 'USER_WAIT'
                                                             WHEN (("lower"(queue_name_from) = 'l1a') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'ESCALATED'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND
                                                                   (("lower"(queue_name_to) = 'l2') OR ("lower"(queue_name_to) = 'escalated'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND ("lower"(queue_name_to) = 'escalated')) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'escalated') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND
                                                                   (("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND ("lower"(queue_name_to) = 'l1a'))
                                                                 THEN 'DEESCALATED' END)                                                       status_to
                                                      , CAST("regexp_extract"(queue_oldvalue, '[0-9]+') AS bigint)                             queue_id_from
                                                      , queue_name_from
                                                      , CAST("regexp_extract"(queue_value, '[0-9]+') AS bigint)                                queue_id_to
                                                      , queue_name_to
                                                      , analyst_id
                                                      , analyst_name
                                                      , action_time_unix                                                                       action_time_unix
                                                      , action_time                                                                            action_time
                                                      , data                                                                                   transaction_data
                                                 FROM (((
                                                     SELECT ticket_id
                                                          , "regexp_extract"(status_str, '"oldValue"[^}]+') status_oldvalue
                                                          , "regexp_extract"(status_str, '"value"[^}]+')    status_value
                                                          , "regexp_extract"(queue_str, '"oldValue"[^}]+')  queue_oldvalue
                                                          , "regexp_extract"(queue_str, '"value"[^}]+')     queue_value
                                                          , status_str
                                                          , analyst_id
                                                          , analyst_name
                                                          , action_time_unix
                                                          , action_time
                                                          , data
                                                     FROM (
                                                              SELECT ticket_id
                                                                   , "regexp_extract"(data, '"field":"Status"[^}]+}')  status_str
                                                                   , "regexp_extract"(data, '"field":"QueueID"[^}]+}') queue_str
                                                                   , creator_id                                        analyst_id
                                                                   , lpu.full_name                                     analyst_name
                                                                   , creation_time                                     action_time_unix
                                                                   , "from_unixtime"(creation_time)                    action_time
                                                                   , data
                                                              FROM ("leon_paycom_transaction:si" lpt
                                                                       LEFT JOIN (
                                                                  SELECT DISTINCT fbid
                                                                                , full_name
                                                                  FROM ("leon_paycom_user:si"
                                                                           INNER JOIN leon_paycom_user_max lpum ON (1 = 1))
                                                                  WHERE (ds = lpum.max_ds)
                                                              ) lpu ON (lpt.creator_id = lpu.fbid))
                                                          )
                                                 ) lpt
                                                     LEFT JOIN (
                                                         SELECT DISTINCT fbid queue_id
                                                                       , name queue_name_from
                                                         FROM ("leon_paycom_queue:si"
                                                                  INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                         WHERE (ds = lpq.max_ds)
                                                     ) qm_old ON (CAST("regexp_extract"(lpt.queue_oldvalue, '[0-9]+') AS bigint) = qm_old.queue_id))
                                                          LEFT JOIN (
                                                     SELECT DISTINCT fbid queue_id
                                                                   , name queue_name_to
                                                     FROM ("leon_paycom_queue:si"
                                                              INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                     WHERE (ds = lpq.max_ds)
                                                 ) qm_new ON (CAST("regexp_extract"(lpt.queue_value, '[0-9]+') AS bigint) = qm_new.queue_id))
                                                 ORDER BY ticket_id ASC, action_time ASC
                                             )
                                        WHERE ((((status_from IS NOT NULL) OR (status_to IS NOT NULL)) OR (queue_name_from IS NOT NULL)) OR
                                               (queue_name_to IS NOT NULL))
                                    )
                           )
                      WHERE ((analyst_name <> 'Leon Paycom Bot') OR (analyst_name IS NULL))
                  ) b
                      INNER JOIN (
                          SELECT (CASE
                                      WHEN (vendor = 0) THEN 'NONE'
                                      WHEN (vendor = 1) THEN 'MANUAL'
                                      WHEN (vendor = 2) THEN 'OTHER'
                                      WHEN (vendor = 3) THEN 'RDC'
                                      WHEN (vendor = 4) THEN 'CSI' END) vendor
                               , ticket_id
                               , inquiry_origin
                               , ds
                          FROM ("staging_paycom_leon_sanction_copy:si"
                                   INNER JOIN staging_paycom_leon_sanction_copy_max splsc_max ON (1 = 1))
                          WHERE (ds = splsc_max.max_ds)
                      ) c ON (b.ticket_id = c.ticket_id))
                           LEFT JOIN (
                      SELECT *
                      FROM (paycom_leon_screening_type_mapping_copy
                               INNER JOIN paycom_leon_screening_type_mapping_copy_max plstm_max ON (1 = 1))
                      WHERE (ds = plstm_max.max_ds)
                  ) d ON (c.inquiry_origin = d.screening_type_enum))
                  ORDER BY b.ticket_id ASC, b.action_time ASC
              )
         WHERE (((((((((("lower"(status_to_revised) = 'escalated') AND (queue_name_from_revised <> queue_name_to_revised)) OR
                       ((("lower"(status_to_revised) = 'open') AND ("lower"(status_from) = 'new')) AND (queue_name_from_revised <> queue_name_to_revised))) OR
                      (("lower"(status_to_revised) = 'open') AND ("lower"(status_from) <> 'new'))) OR
                     ((("lower"(status_to_revised) = 'new') AND ("lower"(status_from) = 'open')) AND (queue_name_from_revised <> queue_name_to_revised))) OR
                    ((("lower"(status_to) = 'user_wait') AND (NOT ("lower"(status_from) IN ('resolved', 'duplicate', 'user_wait')))) AND
                     (queue_name_from_revised = queue_name_to_revised))) OR (("lower"(status_to_revised) = 'new') AND ("lower"(status_from) <> 'open'))) OR
                  ((("lower"(status_to) = 'deescalated') AND ("lower"(status_from) = 'escalated')) AND (queue_name_from_revised <> queue_name_to_revised))) OR
                 ((("lower"(status_to_revised) IN ('resolved', 'user_wait', 'pending', 'duplicate')) AND ("lower"(status_from) IN ('new', 'open'))) AND
                  (queue_name_from_revised <> queue_name_to_revised))) AND ("lower"(queue_name_to) IN ('l1b', 'l2', 'escalated', 'l1a')))
         UNION ALL
         SELECT action_time                ds
              , action_time                action_time
              , queue_name_to_revised      queue_name_to
              , 'Inflow'                   metric_name
              , screening_type_enum_label  screening_type
              , 'Leon'                     system
              , status_to_revised          status_to
              , CAST(ticket_id AS varchar) metric_value
              , vendor                     vendor
         FROM (
                  SELECT b.ticket_id
                       , b.analyst_id
                       , b.analyst_name
                       , b.action_time_unix
                       , b.action_time
                       , b.transaction_data
                       , b.status_from
                       , b.status_to
                       , b.queue_name_from
                       , b.queue_name_to
                       , (CASE
                              WHEN (b.queue_name_to IS NULL)
                                  THEN "first_value"(b.queue_name_to) OVER (PARTITION BY b.ticket_id, b.grp_queue_to ORDER BY b.action_time ASC)
                              ELSE b.queue_name_from END)           queue_name_from_revised
                       , (CASE
                              WHEN (b.queue_name_to IS NULL) THEN "first_value"(b.queue_name_to)
                                                                                OVER (PARTITION BY b.ticket_id, b.grp_queue_from ORDER BY b.action_time ASC)
                              ELSE b.queue_name_to END)             queue_name_to_revised
                       , (CASE
                              WHEN (b.status_to IS NULL) THEN "first_value"(b.status_to) OVER (PARTITION BY b.ticket_id, b.grp_status ORDER BY b.action_time ASC)
                              ELSE b.status_to END)                 status_to_revised
                       , (CASE
                              WHEN (c.inquiry_origin = 59) THEN 'IG_SHOPPING'
                              WHEN (c.inquiry_origin = 60) THEN 'MACHAMP'
                              WHEN (c.inquiry_origin = 61) THEN 'WA_PAID_MESSAGES'
                              WHEN (c.inquiry_origin = 62) THEN 'IG_P4C'
                              WHEN (c.inquiry_origin = 63) THEN 'IG_P4C_BILLING'
                              WHEN (c.inquiry_origin = 64) THEN 'C2C_COMMERCE'
                              WHEN (c.inquiry_origin = 65) THEN 'GLOBAL_POLITICAL_AUTHORIZATION'
                              WHEN (c.inquiry_origin = 66) THEN 'WA_P2P'
                              WHEN (c.inquiry_origin = 67) THEN 'OCULUS_PORTAL_HARDWARE_STORE'
                              WHEN (c.inquiry_origin = 68) THEN 'IG_P4C_INTL'
                              WHEN (c.inquiry_origin = 69) THEN 'B2C_MESSENGER_COMMERCE'
                              WHEN (c.inquiry_origin = 70) THEN 'VIEWPOINTS'
                              WHEN (c.inquiry_origin = 71) THEN 'GEOBLOCKING'
                              WHEN (c.inquiry_origin = 72) THEN 'VENEZUELA'
                              WHEN (c.inquiry_origin = 73) THEN 'WA_P2M_BRAZIL'
                              ELSE d.screening_type_enum_label END) screening_type_enum_label
                       , c.vendor
                  FROM (((
                      SELECT *
                           , "sum"((CASE WHEN (queue_name_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)   grp_queue_to
                           , "sum"((CASE WHEN (queue_name_from IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC) grp_queue_from
                           , "sum"((CASE WHEN (status_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)       grp_status
                      FROM (
                               SELECT ticket_id
                                    , status_from
                                    , status_to
                                    , queue_id_from
                                    , (CASE
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_to = 'OPEN') AND (queue_name_to = 'L1a')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (queue_name_to = 'L0') THEN 'L0'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (QUEUE_ID_from = 1651263241863038) THEN 'L0'
                                           ELSE queue_name_from END)                                                  queue_name_from
                                    , queue_id_to
                                    , (CASE
                                           WHEN ((status_from = 'NEW') AND (status_to = 'OPEN')) THEN 'L1a'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_to IS NULL)) THEN 'L1a'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL)) AND
                                                 ("lag"(status_from, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'OPEN'))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           ELSE queue_name_to END)                                                    queue_name_to
                                    , analyst_id
                                    , analyst_name
                                    , action_time_unix
                                    , action_time
                                    , transaction_data
                                    , "lag"(status_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) lag_status
                               FROM (
                                        SELECT ticket_id
                                             , (CASE
                                                    WHEN (status_from IS NULL) THEN "lag"(status_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                                    ELSE status_from END) status_from
                                             , status_to
                                             , queue_id_from
                                             , queue_name_from
                                             , queue_id_to
                                             , queue_name_to
                                             , analyst_id
                                             , analyst_name
                                             , action_time_unix
                                             , action_time
                                             , transaction_data
                                        FROM (
                                                 SELECT ticket_id
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2006') THEN 'USER_WAIT' END) status_from
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2006') THEN 'USER_WAIT'
                                                             WHEN (("lower"(queue_name_from) = 'l1a') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'ESCALATED'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND
                                                                   (("lower"(queue_name_to) = 'l2') OR ("lower"(queue_name_to) = 'escalated'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND ("lower"(queue_name_to) = 'escalated')) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'escalated') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND
                                                                   (("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND ("lower"(queue_name_to) = 'l1a'))
                                                                 THEN 'DEESCALATED' END)                                                       status_to
                                                      , CAST("regexp_extract"(queue_oldvalue, '[0-9]+') AS bigint)                             queue_id_from
                                                      , queue_name_from
                                                      , CAST("regexp_extract"(queue_value, '[0-9]+') AS bigint)                                queue_id_to
                                                      , queue_name_to
                                                      , analyst_id
                                                      , analyst_name
                                                      , action_time_unix                                                                       action_time_unix
                                                      , action_time                                                                            action_time
                                                      , data                                                                                   transaction_data
                                                 FROM (((
                                                     SELECT ticket_id
                                                          , "regexp_extract"(status_str, '"oldValue"[^}]+') status_oldvalue
                                                          , "regexp_extract"(status_str, '"value"[^}]+')    status_value
                                                          , "regexp_extract"(queue_str, '"oldValue"[^}]+')  queue_oldvalue
                                                          , "regexp_extract"(queue_str, '"value"[^}]+')     queue_value
                                                          , status_str
                                                          , analyst_id
                                                          , analyst_name
                                                          , action_time_unix
                                                          , action_time
                                                          , data
                                                     FROM (
                                                              SELECT ticket_id
                                                                   , "regexp_extract"(data, '"field":"Status"[^}]+}')  status_str
                                                                   , "regexp_extract"(data, '"field":"QueueID"[^}]+}') queue_str
                                                                   , creator_id                                        analyst_id
                                                                   , lpu.full_name                                     analyst_name
                                                                   , creation_time                                     action_time_unix
                                                                   , "from_unixtime"(creation_time)                    action_time
                                                                   , data
                                                              FROM ("leon_paycom_transaction:si" lpt
                                                                       LEFT JOIN (
                                                                  SELECT DISTINCT fbid
                                                                                , full_name
                                                                  FROM ("leon_paycom_user:si"
                                                                           INNER JOIN leon_paycom_user_max lpum ON (1 = 1))
                                                                  WHERE (ds = lpum.max_ds)
                                                              ) lpu ON (lpt.creator_id = lpu.fbid))
                                                          )
                                                 ) lpt
                                                     LEFT JOIN (
                                                         SELECT DISTINCT fbid queue_id
                                                                       , name queue_name_from
                                                         FROM ("leon_paycom_queue:si"
                                                                  INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                         WHERE (ds = lpq.max_ds)
                                                     ) qm_old ON (CAST("regexp_extract"(lpt.queue_oldvalue, '[0-9]+') AS bigint) = qm_old.queue_id))
                                                          LEFT JOIN (
                                                     SELECT DISTINCT fbid queue_id
                                                                   , name queue_name_to
                                                     FROM ("leon_paycom_queue:si"
                                                              INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                     WHERE (ds = lpq.max_ds)
                                                 ) qm_new ON (CAST("regexp_extract"(lpt.queue_value, '[0-9]+') AS bigint) = qm_new.queue_id))
                                                 ORDER BY ticket_id ASC, action_time ASC
                                             )
                                        WHERE ((((status_from IS NOT NULL) OR (status_to IS NOT NULL)) OR (queue_name_from IS NOT NULL)) OR
                                               (queue_name_to IS NOT NULL))
                                    )
                           )
                      WHERE ((analyst_name <> 'Leon Paycom Bot') OR (analyst_name IS NULL))
                  ) b
                      INNER JOIN (
                          SELECT (CASE
                                      WHEN (vendor = 0) THEN 'NONE'
                                      WHEN (vendor = 1) THEN 'MANUAL'
                                      WHEN (vendor = 2) THEN 'OTHER'
                                      WHEN (vendor = 3) THEN 'RDC'
                                      WHEN (vendor = 4) THEN 'CSI' END) vendor
                               , ticket_id
                               , inquiry_origin
                               , ds
                          FROM ("staging_paycom_leon_sanction_copy:si"
                                   INNER JOIN staging_paycom_leon_sanction_copy_max splsc_max ON (1 = 1))
                          WHERE (ds = splsc_max.max_ds)
                      ) c ON (b.ticket_id = c.ticket_id))
                           LEFT JOIN (
                      SELECT *
                      FROM (paycom_leon_screening_type_mapping_copy
                               INNER JOIN paycom_leon_screening_type_mapping_copy_max plstm_max ON (1 = 1))
                      WHERE (ds = plstm_max.max_ds)
                  ) d ON (c.inquiry_origin = d.screening_type_enum))
                  WHERE ((("lower"(b.status_to) = 'resolved') AND (b.lag_status <> "lower"(b.status_to))) AND (b.queue_name_from <> b.queue_name_to))
                  ORDER BY b.ticket_id ASC, b.action_time ASC
              )
         UNION ALL
         SELECT action_time                ds
              , action_time                action_time
              , queue_name_to_revised      queue_name_to
              , 'Outflow'                  metric_name
              , screening_type_enum_label  screening_type
              , 'Leon'                     system
              , status_to_revised          status_to
              , CAST(ticket_id AS varchar) metric_value
              , vendor                     vendor
         FROM (
                  SELECT b.ticket_id
                       , b.analyst_id
                       , b.analyst_name
                       , b.action_time_unix
                       , b.action_time
                       , b.transaction_data
                       , b.status_from
                       , b.status_to
                       , b.queue_name_from
                       , b.queue_name_to
                       , (CASE
                              WHEN (b.queue_name_to IS NULL)
                                  THEN "first_value"(b.queue_name_to) OVER (PARTITION BY b.ticket_id, b.grp_queue_to ORDER BY b.action_time ASC)
                              ELSE b.queue_name_from END)           queue_name_from_revised
                       , (CASE
                              WHEN (b.queue_name_to IS NULL) THEN "first_value"(b.queue_name_to)
                                                                                OVER (PARTITION BY b.ticket_id, b.grp_queue_from ORDER BY b.action_time ASC)
                              ELSE b.queue_name_to END)             queue_name_to_revised
                       , (CASE
                              WHEN (b.status_to IS NULL) THEN "first_value"(b.status_to) OVER (PARTITION BY b.ticket_id, b.grp_status ORDER BY b.action_time ASC)
                              ELSE b.status_to END)                 status_to_revised
                       , (CASE
                              WHEN (c.inquiry_origin = 59) THEN 'IG_SHOPPING'
                              WHEN (c.inquiry_origin = 60) THEN 'MACHAMP'
                              WHEN (c.inquiry_origin = 61) THEN 'WA_PAID_MESSAGES'
                              WHEN (c.inquiry_origin = 62) THEN 'IG_P4C'
                              WHEN (c.inquiry_origin = 63) THEN 'IG_P4C_BILLING'
                              WHEN (c.inquiry_origin = 64) THEN 'C2C_COMMERCE'
                              WHEN (c.inquiry_origin = 65) THEN 'GLOBAL_POLITICAL_AUTHORIZATION'
                              WHEN (c.inquiry_origin = 66) THEN 'WA_P2P'
                              WHEN (c.inquiry_origin = 67) THEN 'OCULUS_PORTAL_HARDWARE_STORE'
                              WHEN (c.inquiry_origin = 68) THEN 'IG_P4C_INTL'
                              WHEN (c.inquiry_origin = 69) THEN 'B2C_MESSENGER_COMMERCE'
                              WHEN (c.inquiry_origin = 70) THEN 'VIEWPOINTS'
                              WHEN (c.inquiry_origin = 71) THEN 'GEOBLOCKING'
                              WHEN (c.inquiry_origin = 72) THEN 'VENEZUELA'
                              WHEN (c.inquiry_origin = 73) THEN 'WA_P2M_BRAZIL'
                              ELSE d.screening_type_enum_label END) screening_type_enum_label
                       , c.vendor
                  FROM (((
                      SELECT *
                           , "sum"((CASE WHEN (queue_name_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)   grp_queue_to
                           , "sum"((CASE WHEN (queue_name_from IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC) grp_queue_from
                           , "sum"((CASE WHEN (status_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)       grp_status
                      FROM (
                               SELECT ticket_id
                                    , status_from
                                    , status_to
                                    , queue_id_from
                                    , (CASE
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'USER_WAIT')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_to = 'OPEN') AND (queue_name_to = 'L1a')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (queue_name_to = 'L0') THEN 'L0'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (QUEUE_ID_from = 1651263241863038) THEN 'L0'
                                           ELSE queue_name_from END)                                                  queue_name_from
                                    , queue_id_to
                                    , (CASE
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'USER_WAIT')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((status_from = 'NEW') AND (status_to = 'OPEN')) THEN 'L1a'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(status_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'NEW'))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_to IS NULL)) THEN 'L1a'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL)) AND
                                                 ("lag"(status_from, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'OPEN'))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           ELSE queue_name_to END)                                                    queue_name_to
                                    , analyst_id
                                    , analyst_name
                                    , action_time_unix
                                    , action_time
                                    , transaction_data
                                    , "lag"(status_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) lag_status
                               FROM (
                                        SELECT ticket_id
                                             , (CASE
                                                    WHEN (status_from IS NULL) THEN "lag"(status_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                                    ELSE status_from END) status_from
                                             , status_to
                                             , queue_id_from
                                             , queue_name_from
                                             , queue_id_to
                                             , queue_name_to
                                             , analyst_id
                                             , analyst_name
                                             , action_time_unix
                                             , action_time
                                             , transaction_data
                                        FROM (
                                                 SELECT ticket_id
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2006') THEN 'USER_WAIT' END) status_from
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2006') THEN 'USER_WAIT'
                                                             WHEN (("lower"(queue_name_from) = 'l1a') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'ESCALATED'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND
                                                                   (("lower"(queue_name_to) = 'l2') OR ("lower"(queue_name_to) = 'escalated'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND ("lower"(queue_name_to) = 'escalated')) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'escalated') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND
                                                                   (("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND ("lower"(queue_name_to) = 'l1a'))
                                                                 THEN 'DEESCALATED' END)                                                       status_to
                                                      , CAST("regexp_extract"(queue_oldvalue, '[0-9]+') AS bigint)                             queue_id_from
                                                      , queue_name_from
                                                      , CAST("regexp_extract"(queue_value, '[0-9]+') AS bigint)                                queue_id_to
                                                      , queue_name_to
                                                      , analyst_id
                                                      , analyst_name
                                                      , action_time_unix                                                                       action_time_unix
                                                      , action_time                                                                            action_time
                                                      , data                                                                                   transaction_data
                                                 FROM (((
                                                     SELECT ticket_id
                                                          , "regexp_extract"(status_str, '"oldValue"[^}]+') status_oldvalue
                                                          , "regexp_extract"(status_str, '"value"[^}]+')    status_value
                                                          , "regexp_extract"(queue_str, '"oldValue"[^}]+')  queue_oldvalue
                                                          , "regexp_extract"(queue_str, '"value"[^}]+')     queue_value
                                                          , status_str
                                                          , analyst_id
                                                          , analyst_name
                                                          , action_time_unix
                                                          , action_time
                                                          , data
                                                     FROM (
                                                              SELECT ticket_id
                                                                   , "regexp_extract"(data, '"field":"Status"[^}]+}')  status_str
                                                                   , "regexp_extract"(data, '"field":"QueueID"[^}]+}') queue_str
                                                                   , creator_id                                        analyst_id
                                                                   , lpu.full_name                                     analyst_name
                                                                   , creation_time                                     action_time_unix
                                                                   , "from_unixtime"(creation_time)                    action_time
                                                                   , data
                                                              FROM ("leon_paycom_transaction:si" lpt
                                                                       LEFT JOIN (
                                                                  SELECT DISTINCT fbid
                                                                                , full_name
                                                                  FROM ("leon_paycom_user:si"
                                                                           INNER JOIN leon_paycom_user_max lpum ON (1 = 1))
                                                                  WHERE (ds = lpum.max_ds)
                                                              ) lpu ON (lpt.creator_id = lpu.fbid))
                                                          )
                                                 ) lpt
                                                     LEFT JOIN (
                                                         SELECT DISTINCT fbid queue_id
                                                                       , name queue_name_from
                                                         FROM ("leon_paycom_queue:si"
                                                                  INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                         WHERE (ds = lpq.max_ds)
                                                     ) qm_old ON (CAST("regexp_extract"(lpt.queue_oldvalue, '[0-9]+') AS bigint) = qm_old.queue_id))
                                                          LEFT JOIN (
                                                     SELECT DISTINCT fbid queue_id
                                                                   , name queue_name_to
                                                     FROM ("leon_paycom_queue:si"
                                                              INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                     WHERE (ds = lpq.max_ds)
                                                 ) qm_new ON (CAST("regexp_extract"(lpt.queue_value, '[0-9]+') AS bigint) = qm_new.queue_id))
                                                 ORDER BY ticket_id ASC, action_time ASC
                                             )
                                        WHERE ((((status_from IS NOT NULL) OR (status_to IS NOT NULL)) OR (queue_name_from IS NOT NULL)) OR
                                               (queue_name_to IS NOT NULL))
                                    )
                           )
                      WHERE ((analyst_name <> 'Leon Paycom Bot') OR (analyst_name IS NULL))
                  ) b
                      INNER JOIN (
                          SELECT (CASE
                                      WHEN (vendor = 0) THEN 'NONE'
                                      WHEN (vendor = 1) THEN 'MANUAL'
                                      WHEN (vendor = 2) THEN 'OTHER'
                                      WHEN (vendor = 3) THEN 'RDC'
                                      WHEN (vendor = 4) THEN 'CSI' END) vendor
                               , ticket_id
                               , inquiry_origin
                               , ds
                          FROM ("staging_paycom_leon_sanction_copy:si"
                                   INNER JOIN staging_paycom_leon_sanction_copy_max splsc_max ON (1 = 1))
                          WHERE (ds = splsc_max.max_ds)
                      ) c ON (b.ticket_id = c.ticket_id))
                           LEFT JOIN (
                      SELECT *
                      FROM (paycom_leon_screening_type_mapping_copy
                               INNER JOIN paycom_leon_screening_type_mapping_copy_max plstm_max ON (1 = 1))
                      WHERE (ds = plstm_max.max_ds)
                  ) d ON (c.inquiry_origin = d.screening_type_enum))
                  WHERE ((("lower"(b.status_to) = 'resolved') AND (b.lag_status <> "lower"(b.status_to))) AND (b.queue_name_from <> b.queue_name_to))
                  ORDER BY b.ticket_id ASC, b.action_time ASC
              )
         UNION ALL
         SELECT action_time                ds
              , action_time                action_time
              , queue_name_to_revised      queue_name_to
              , 'Outflow'                  metric_name
              , screening_type_enum_label  screening_type
              , 'Leon'                     system
              , status_to_revised          status_to
              , CAST(ticket_id AS varchar) metric_value
              , vendor                     vendor
         FROM (
                  SELECT b.ticket_id
                       , b.analyst_id
                       , b.analyst_name
                       , b.action_time_unix
                       , b.action_time
                       , b.transaction_data
                       , b.status_from
                       , b.status_to
                       , b.queue_name_from
                       , b.queue_name_to
                       , (CASE
                              WHEN (b.queue_name_to IS NULL)
                                  THEN "first_value"(b.queue_name_to) OVER (PARTITION BY b.ticket_id, b.grp_queue_to ORDER BY b.action_time ASC)
                              ELSE b.queue_name_from END)           queue_name_from_revised
                       , (CASE
                              WHEN (b.queue_name_to IS NULL) THEN "first_value"(b.queue_name_to)
                                                                                OVER (PARTITION BY b.ticket_id, b.grp_queue_from ORDER BY b.action_time ASC)
                              ELSE b.queue_name_to END)             queue_name_to_revised
                       , (CASE
                              WHEN (b.status_to IS NULL) THEN "first_value"(b.status_to) OVER (PARTITION BY b.ticket_id, b.grp_status ORDER BY b.action_time ASC)
                              ELSE b.status_to END)                 status_to_revised
                       , (CASE
                              WHEN (c.inquiry_origin = 59) THEN 'IG_SHOPPING'
                              WHEN (c.inquiry_origin = 60) THEN 'MACHAMP'
                              WHEN (c.inquiry_origin = 61) THEN 'WA_PAID_MESSAGES'
                              WHEN (c.inquiry_origin = 62) THEN 'IG_P4C'
                              WHEN (c.inquiry_origin = 63) THEN 'IG_P4C_BILLING'
                              WHEN (c.inquiry_origin = 64) THEN 'C2C_COMMERCE'
                              WHEN (c.inquiry_origin = 65) THEN 'GLOBAL_POLITICAL_AUTHORIZATION'
                              WHEN (c.inquiry_origin = 66) THEN 'WA_P2P'
                              WHEN (c.inquiry_origin = 67) THEN 'OCULUS_PORTAL_HARDWARE_STORE'
                              WHEN (c.inquiry_origin = 68) THEN 'IG_P4C_INTL'
                              WHEN (c.inquiry_origin = 69) THEN 'B2C_MESSENGER_COMMERCE'
                              WHEN (c.inquiry_origin = 70) THEN 'VIEWPOINTS'
                              WHEN (c.inquiry_origin = 71) THEN 'GEOBLOCKING'
                              WHEN (c.inquiry_origin = 72) THEN 'VENEZUELA'
                              WHEN (c.inquiry_origin = 73) THEN 'WA_P2M_BRAZIL'
                              ELSE d.screening_type_enum_label END) screening_type_enum_label
                       , c.vendor
                  FROM (((
                      SELECT *
                           , "sum"((CASE WHEN (queue_name_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)   grp_queue_to
                           , "sum"((CASE WHEN (queue_name_from IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC) grp_queue_from
                           , "sum"((CASE WHEN (status_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)       grp_status
                      FROM (
                               SELECT ticket_id
                                    , status_from
                                    , status_to
                                    , queue_id_from
                                    , (CASE
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'USER_WAIT')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_to = 'OPEN') AND (queue_name_to = 'L1a')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(status_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'NEW'))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (queue_name_to = 'L0') THEN 'L0'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (QUEUE_ID_from = 1651263241863038) THEN 'L0'
                                           ELSE queue_name_from END) queue_name_from
                                    , queue_id_to
                                    , (CASE
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'USER_WAIT')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((status_from = 'NEW') AND (status_to = 'OPEN')) THEN 'L1a'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_to IS NULL)) THEN 'L1a'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL)) AND
                                                 ("lag"(status_from, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'OPEN'))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           ELSE queue_name_to END)   queue_name_to
                                    , analyst_id
                                    , analyst_name
                                    , action_time_unix
                                    , action_time
                                    , transaction_data
                               FROM (
                                        SELECT ticket_id
                                             , (CASE
                                                    WHEN (status_from IS NULL) THEN "lag"(status_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                                    ELSE status_from END) status_from
                                             , status_to
                                             , queue_id_from
                                             , queue_name_from
                                             , queue_id_to
                                             , queue_name_to
                                             , analyst_id
                                             , analyst_name
                                             , action_time_unix
                                             , action_time
                                             , transaction_data
                                        FROM (
                                                 SELECT ticket_id
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2006') THEN 'USER_WAIT' END) status_from
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2006') THEN 'USER_WAIT'
                                                             WHEN (("lower"(queue_name_from) = 'l1a') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'ESCALATED'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND
                                                                   (("lower"(queue_name_to) = 'l2') OR ("lower"(queue_name_to) = 'escalated'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND ("lower"(queue_name_to) = 'escalated')) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'escalated') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND
                                                                   (("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND ("lower"(queue_name_to) = 'l1a'))
                                                                 THEN 'DEESCALATED' END)                                                       status_to
                                                      , CAST("regexp_extract"(queue_oldvalue, '[0-9]+') AS bigint)                             queue_id_from
                                                      , queue_name_from
                                                      , CAST("regexp_extract"(queue_value, '[0-9]+') AS bigint)                                queue_id_to
                                                      , queue_name_to
                                                      , analyst_id
                                                      , analyst_name
                                                      , action_time_unix                                                                       action_time_unix
                                                      , action_time                                                                            action_time
                                                      , data                                                                                   transaction_data
                                                 FROM (((
                                                     SELECT ticket_id
                                                          , "regexp_extract"(status_str, '"oldValue"[^}]+') status_oldvalue
                                                          , "regexp_extract"(status_str, '"value"[^}]+')    status_value
                                                          , "regexp_extract"(queue_str, '"oldValue"[^}]+')  queue_oldvalue
                                                          , "regexp_extract"(queue_str, '"value"[^}]+')     queue_value
                                                          , status_str
                                                          , analyst_id
                                                          , analyst_name
                                                          , action_time_unix
                                                          , action_time
                                                          , data
                                                     FROM (
                                                              SELECT ticket_id
                                                                   , "regexp_extract"(data, '"field":"Status"[^}]+}')  status_str
                                                                   , "regexp_extract"(data, '"field":"QueueID"[^}]+}') queue_str
                                                                   , creator_id                                        analyst_id
                                                                   , lpu.full_name                                     analyst_name
                                                                   , creation_time                                     action_time_unix
                                                                   , "from_unixtime"(creation_time)                    action_time
                                                                   , data
                                                              FROM ("leon_paycom_transaction:si" lpt
                                                                       LEFT JOIN (
                                                                  SELECT DISTINCT fbid
                                                                                , full_name
                                                                  FROM ("leon_paycom_user:si"
                                                                           INNER JOIN leon_paycom_user_max lpum ON (1 = 1))
                                                                  WHERE (ds = lpum.max_ds)
                                                              ) lpu ON (lpt.creator_id = lpu.fbid))
                                                          )
                                                 ) lpt
                                                     LEFT JOIN (
                                                         SELECT DISTINCT fbid queue_id
                                                                       , name queue_name_from
                                                         FROM ("leon_paycom_queue:si"
                                                                  INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                         WHERE (ds = lpq.max_ds)
                                                     ) qm_old ON (CAST("regexp_extract"(lpt.queue_oldvalue, '[0-9]+') AS bigint) = qm_old.queue_id))
                                                          LEFT JOIN (
                                                     SELECT DISTINCT fbid queue_id
                                                                   , name queue_name_to
                                                     FROM ("leon_paycom_queue:si"
                                                              INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                     WHERE (ds = lpq.max_ds)
                                                 ) qm_new ON (CAST("regexp_extract"(lpt.queue_value, '[0-9]+') AS bigint) = qm_new.queue_id))
                                                 ORDER BY ticket_id ASC, action_time ASC
                                             )
                                        WHERE ((((status_from IS NOT NULL) OR (status_to IS NOT NULL)) OR (queue_name_from IS NOT NULL)) OR
                                               (queue_name_to IS NOT NULL))
                                    )
                           )
                      WHERE ((analyst_name <> 'Leon Paycom Bot') OR (analyst_name IS NULL))
                  ) b
                      INNER JOIN (
                          SELECT (CASE
                                      WHEN (vendor = 0) THEN 'NONE'
                                      WHEN (vendor = 1) THEN 'MANUAL'
                                      WHEN (vendor = 2) THEN 'OTHER'
                                      WHEN (vendor = 3) THEN 'RDC'
                                      WHEN (vendor = 4) THEN 'CSI' END) vendor
                               , ticket_id
                               , inquiry_origin
                               , ds
                          FROM ("staging_paycom_leon_sanction_copy:si"
                                   INNER JOIN staging_paycom_leon_sanction_copy_max splsc_max ON (1 = 1))
                          WHERE (ds = splsc_max.max_ds)
                      ) c ON (b.ticket_id = c.ticket_id))
                           LEFT JOIN (
                      SELECT *
                      FROM (paycom_leon_screening_type_mapping_copy
                               INNER JOIN paycom_leon_screening_type_mapping_copy_max plstm_max ON (1 = 1))
                      WHERE (ds = plstm_max.max_ds)
                  ) d ON (c.inquiry_origin = d.screening_type_enum))
                  ORDER BY b.ticket_id ASC, b.action_time ASC
              )
         WHERE ((("lower"(status_to_revised) IN ('user_wait', 'duplicate')) AND ("lower"(status_from) IN ('new', 'open'))) AND
                (queue_name_from_revised <> queue_name_to_revised))
         UNION ALL
         SELECT action_time                ds
              , action_time                action_time
              , queue_name_to_revised      queue_name_to
              , 'Outflow'                  metric_name
              , screening_type_enum_label  screening_type
              , 'Leon'                     system
              , status_to_revised          status_to
              , CAST(ticket_id AS varchar) metric_value
              , vendor                     vendor
         FROM (
                  SELECT b.ticket_id
                       , b.analyst_id
                       , b.analyst_name
                       , b.action_time_unix
                       , b.action_time
                       , b.transaction_data
                       , b.status_from
                       , b.status_to
                       , b.queue_name_from
                       , b.queue_name_to
                       , (CASE
                              WHEN (b.queue_name_to IS NULL)
                                  THEN "first_value"(b.queue_name_to) OVER (PARTITION BY b.ticket_id, b.grp_queue_to ORDER BY b.action_time ASC)
                              ELSE b.queue_name_from END)           queue_name_from_revised
                       , (CASE
                              WHEN (b.queue_name_to IS NULL) THEN "first_value"(b.queue_name_to)
                                                                                OVER (PARTITION BY b.ticket_id, b.grp_queue_from ORDER BY b.action_time ASC)
                              ELSE b.queue_name_to END)             queue_name_to_revised
                       , (CASE
                              WHEN (b.status_to IS NULL) THEN "first_value"(b.status_to) OVER (PARTITION BY b.ticket_id, b.grp_status ORDER BY b.action_time ASC)
                              ELSE b.status_to END)                 status_to_revised
                       , (CASE
                              WHEN (c.inquiry_origin = 59) THEN 'IG_SHOPPING'
                              WHEN (c.inquiry_origin = 60) THEN 'MACHAMP'
                              WHEN (c.inquiry_origin = 61) THEN 'WA_PAID_MESSAGES'
                              WHEN (c.inquiry_origin = 62) THEN 'IG_P4C'
                              WHEN (c.inquiry_origin = 63) THEN 'IG_P4C_BILLING'
                              WHEN (c.inquiry_origin = 64) THEN 'C2C_COMMERCE'
                              WHEN (c.inquiry_origin = 65) THEN 'GLOBAL_POLITICAL_AUTHORIZATION'
                              WHEN (c.inquiry_origin = 66) THEN 'WA_P2P'
                              WHEN (c.inquiry_origin = 67) THEN 'OCULUS_PORTAL_HARDWARE_STORE'
                              WHEN (c.inquiry_origin = 68) THEN 'IG_P4C_INTL'
                              WHEN (c.inquiry_origin = 69) THEN 'B2C_MESSENGER_COMMERCE'
                              WHEN (c.inquiry_origin = 70) THEN 'VIEWPOINTS'
                              WHEN (c.inquiry_origin = 71) THEN 'GEOBLOCKING'
                              WHEN (c.inquiry_origin = 72) THEN 'VENEZUELA'
                              WHEN (c.inquiry_origin = 73) THEN 'WA_P2M_BRAZIL'
                              ELSE d.screening_type_enum_label END) screening_type_enum_label
                       , c.vendor
                  FROM (((
                      SELECT *
                           , "sum"((CASE WHEN (queue_name_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)   grp_queue_to
                           , "sum"((CASE WHEN (queue_name_from IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC) grp_queue_from
                           , "sum"((CASE WHEN (status_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)       grp_status
                      FROM (
                               SELECT ticket_id
                                    , status_from
                                    , status_to
                                    , queue_id_from
                                    , (CASE
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'USER_WAIT')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_to = 'OPEN') AND (queue_name_to = 'L1a')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (queue_name_to = 'L0') THEN 'L0'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (QUEUE_ID_from = 1651263241863038) THEN 'L0'
                                           ELSE queue_name_from END) queue_name_from
                                    , queue_id_to
                                    , (CASE
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'USER_WAIT')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((status_from = 'NEW') AND (status_to = 'OPEN')) THEN 'L1a'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(status_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'NEW'))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_to IS NULL)) THEN 'L1a'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL)) AND
                                                 ("lag"(status_from, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'OPEN'))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           ELSE queue_name_to END)   queue_name_to
                                    , analyst_id
                                    , analyst_name
                                    , action_time_unix
                                    , action_time
                                    , transaction_data
                               FROM (
                                        SELECT ticket_id
                                             , (CASE
                                                    WHEN (status_from IS NULL) THEN "lag"(status_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                                    ELSE status_from END) status_from
                                             , status_to
                                             , queue_id_from
                                             , queue_name_from
                                             , queue_id_to
                                             , queue_name_to
                                             , analyst_id
                                             , analyst_name
                                             , action_time_unix
                                             , action_time
                                             , transaction_data
                                        FROM (
                                                 SELECT ticket_id
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2006') THEN 'USER_WAIT' END) status_from
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2006') THEN 'USER_WAIT'
                                                             WHEN (("lower"(queue_name_from) = 'l1a') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'ESCALATED'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND
                                                                   (("lower"(queue_name_to) = 'l2') OR ("lower"(queue_name_to) = 'escalated'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND ("lower"(queue_name_to) = 'escalated')) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'escalated') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND
                                                                   (("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND ("lower"(queue_name_to) = 'l1a'))
                                                                 THEN 'DEESCALATED' END)                                                       status_to
                                                      , CAST("regexp_extract"(queue_oldvalue, '[0-9]+') AS bigint)                             queue_id_from
                                                      , queue_name_from
                                                      , CAST("regexp_extract"(queue_value, '[0-9]+') AS bigint)                                queue_id_to
                                                      , queue_name_to
                                                      , analyst_id
                                                      , analyst_name
                                                      , action_time_unix                                                                       action_time_unix
                                                      , action_time                                                                            action_time
                                                      , data                                                                                   transaction_data
                                                 FROM (((
                                                     SELECT ticket_id
                                                          , "regexp_extract"(status_str, '"oldValue"[^}]+') status_oldvalue
                                                          , "regexp_extract"(status_str, '"value"[^}]+')    status_value
                                                          , "regexp_extract"(queue_str, '"oldValue"[^}]+')  queue_oldvalue
                                                          , "regexp_extract"(queue_str, '"value"[^}]+')     queue_value
                                                          , status_str
                                                          , analyst_id
                                                          , analyst_name
                                                          , action_time_unix
                                                          , action_time
                                                          , data
                                                     FROM (
                                                              SELECT ticket_id
                                                                   , "regexp_extract"(data, '"field":"Status"[^}]+}')  status_str
                                                                   , "regexp_extract"(data, '"field":"QueueID"[^}]+}') queue_str
                                                                   , creator_id                                        analyst_id
                                                                   , lpu.full_name                                     analyst_name
                                                                   , creation_time                                     action_time_unix
                                                                   , "from_unixtime"(creation_time)                    action_time
                                                                   , data
                                                              FROM ("leon_paycom_transaction:si" lpt
                                                                       LEFT JOIN (
                                                                  SELECT DISTINCT fbid
                                                                                , full_name
                                                                  FROM ("leon_paycom_user:si"
                                                                           INNER JOIN leon_paycom_user_max lpum ON (1 = 1))
                                                                  WHERE (ds = lpum.max_ds)
                                                              ) lpu ON (lpt.creator_id = lpu.fbid))
                                                          )
                                                 ) lpt
                                                     LEFT JOIN (
                                                         SELECT DISTINCT fbid queue_id
                                                                       , name queue_name_from
                                                         FROM ("leon_paycom_queue:si"
                                                                  INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                         WHERE (ds = lpq.max_ds)
                                                     ) qm_old ON (CAST("regexp_extract"(lpt.queue_oldvalue, '[0-9]+') AS bigint) = qm_old.queue_id))
                                                          LEFT JOIN (
                                                     SELECT DISTINCT fbid queue_id
                                                                   , name queue_name_to
                                                     FROM ("leon_paycom_queue:si"
                                                              INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                     WHERE (ds = lpq.max_ds)
                                                 ) qm_new ON (CAST("regexp_extract"(lpt.queue_value, '[0-9]+') AS bigint) = qm_new.queue_id))
                                                 ORDER BY ticket_id ASC, action_time ASC
                                             )
                                        WHERE ((((status_from IS NOT NULL) OR (status_to IS NOT NULL)) OR (queue_name_from IS NOT NULL)) OR
                                               (queue_name_to IS NOT NULL))
                                    )
                           )
                      WHERE ((analyst_name <> 'Leon Paycom Bot') OR (analyst_name IS NULL))
                  ) b
                      INNER JOIN (
                          SELECT (CASE
                                      WHEN (vendor = 0) THEN 'NONE'
                                      WHEN (vendor = 1) THEN 'MANUAL'
                                      WHEN (vendor = 2) THEN 'OTHER'
                                      WHEN (vendor = 3) THEN 'RDC'
                                      WHEN (vendor = 4) THEN 'CSI' END) vendor
                               , ticket_id
                               , inquiry_origin
                               , ds
                          FROM ("staging_paycom_leon_sanction_copy:si"
                                   INNER JOIN staging_paycom_leon_sanction_copy_max splsc_max ON (1 = 1))
                          WHERE (ds = splsc_max.max_ds)
                      ) c ON (b.ticket_id = c.ticket_id))
                           LEFT JOIN (
                      SELECT *
                      FROM (paycom_leon_screening_type_mapping_copy
                               INNER JOIN paycom_leon_screening_type_mapping_copy_max plstm_max ON (1 = 1))
                      WHERE (ds = plstm_max.max_ds)
                  ) d ON (c.inquiry_origin = d.screening_type_enum))
                  ORDER BY b.ticket_id ASC, b.action_time ASC
              )
         WHERE ((("lower"(status_to_revised) IN ('user_wait', 'duplicate')) AND ("lower"(status_from) IN ('new', 'open'))) AND
                (queue_name_from_revised <> queue_name_to_revised))
         UNION ALL
         SELECT action_time                ds
              , action_time                action_time
              , queue_name_from_revised    queue_name_to
              , 'Outflow'                  metric_name
              , screening_type_enum_label  screening_type
              , 'Leon'                     system
              , status_to_revised          status_to
              , CAST(ticket_id AS varchar) metric_value
              , vendor                     vendor
         FROM (
                  SELECT b.ticket_id
                       , b.analyst_id
                       , b.analyst_name
                       , b.action_time_unix
                       , b.action_time
                       , b.transaction_data
                       , b.status_from
                       , b.status_to
                       , b.queue_name_from
                       , b.queue_name_to
                       , (CASE
                              WHEN (b.queue_name_to IS NULL)
                                  THEN "first_value"(b.queue_name_to) OVER (PARTITION BY b.ticket_id, b.grp_queue_to ORDER BY b.action_time ASC)
                              ELSE b.queue_name_from END)           queue_name_from_revised
                       , (CASE
                              WHEN (b.queue_name_to IS NULL) THEN "first_value"(b.queue_name_to)
                                                                                OVER (PARTITION BY b.ticket_id, b.grp_queue_from ORDER BY b.action_time ASC)
                              ELSE b.queue_name_to END)             queue_name_to_revised
                       , (CASE
                              WHEN (b.status_to IS NULL) THEN "first_value"(b.status_to) OVER (PARTITION BY b.ticket_id, b.grp_status ORDER BY b.action_time ASC)
                              ELSE b.status_to END)                 status_to_revised
                       , (CASE
                              WHEN (c.inquiry_origin = 59) THEN 'IG_SHOPPING'
                              WHEN (c.inquiry_origin = 60) THEN 'MACHAMP'
                              WHEN (c.inquiry_origin = 61) THEN 'WA_PAID_MESSAGES'
                              WHEN (c.inquiry_origin = 62) THEN 'IG_P4C'
                              WHEN (c.inquiry_origin = 63) THEN 'IG_P4C_BILLING'
                              WHEN (c.inquiry_origin = 64) THEN 'C2C_COMMERCE'
                              WHEN (c.inquiry_origin = 65) THEN 'GLOBAL_POLITICAL_AUTHORIZATION'
                              WHEN (c.inquiry_origin = 66) THEN 'WA_P2P'
                              WHEN (c.inquiry_origin = 67) THEN 'OCULUS_PORTAL_HARDWARE_STORE'
                              WHEN (c.inquiry_origin = 68) THEN 'IG_P4C_INTL'
                              WHEN (c.inquiry_origin = 69) THEN 'B2C_MESSENGER_COMMERCE'
                              WHEN (c.inquiry_origin = 70) THEN 'VIEWPOINTS'
                              WHEN (c.inquiry_origin = 71) THEN 'GEOBLOCKING'
                              WHEN (c.inquiry_origin = 72) THEN 'VENEZUELA'
                              WHEN (c.inquiry_origin = 73) THEN 'WA_P2M_BRAZIL'
                              ELSE d.screening_type_enum_label END) screening_type_enum_label
                       , c.vendor
                  FROM (((
                      SELECT *
                           , "sum"((CASE WHEN (queue_name_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)   grp_queue_to
                           , "sum"((CASE WHEN (queue_name_from IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC) grp_queue_from
                           , "sum"((CASE WHEN (status_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)       grp_status
                      FROM (
                               SELECT ticket_id
                                    , status_from
                                    , status_to
                                    , queue_id_from
                                    , (CASE
                                           WHEN ((((status_from = 'DUPLICATE') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'USER_WAIT')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_to = 'OPEN') AND (queue_name_to = 'L1a')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (queue_name_to = 'L0') THEN 'L0'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (QUEUE_ID_from = 1651263241863038) THEN 'L0'
                                           ELSE queue_name_from END) queue_name_from
                                    , queue_id_to
                                    , (CASE
                                           WHEN ((((status_from = 'DUPLICATE') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'USER_WAIT')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(status_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'NEW'))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_to IS NULL)) THEN 'L1a'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL)) AND
                                                 ("lag"(status_from, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IN ('OPEN', 'PENDING')))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           ELSE queue_name_to END)   queue_name_to
                                    , analyst_id
                                    , analyst_name
                                    , action_time_unix
                                    , action_time
                                    , transaction_data
                               FROM (
                                        SELECT ticket_id
                                             , (CASE
                                                    WHEN (status_from IS NULL) THEN "lag"(status_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                                    ELSE status_from END) status_from
                                             , status_to
                                             , queue_id_from
                                             , queue_name_from
                                             , queue_id_to
                                             , queue_name_to
                                             , analyst_id
                                             , analyst_name
                                             , action_time_unix
                                             , action_time
                                             , transaction_data
                                        FROM (
                                                 SELECT ticket_id
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2006') THEN 'USER_WAIT' END) status_from
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2006') THEN 'USER_WAIT'
                                                             WHEN (("lower"(queue_name_from) = 'l1a') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'ESCALATED'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND
                                                                   (("lower"(queue_name_to) = 'l2') OR ("lower"(queue_name_to) = 'escalated'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND ("lower"(queue_name_to) = 'escalated')) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'escalated') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND
                                                                   (("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND ("lower"(queue_name_to) = 'l1a'))
                                                                 THEN 'DEESCALATED' END)                                                       status_to
                                                      , CAST("regexp_extract"(queue_oldvalue, '[0-9]+') AS bigint)                             queue_id_from
                                                      , queue_name_from
                                                      , CAST("regexp_extract"(queue_value, '[0-9]+') AS bigint)                                queue_id_to
                                                      , queue_name_to
                                                      , analyst_id
                                                      , analyst_name
                                                      , action_time_unix                                                                       action_time_unix
                                                      , action_time                                                                            action_time
                                                      , data                                                                                   transaction_data
                                                 FROM (((
                                                     SELECT ticket_id
                                                          , "regexp_extract"(status_str, '"oldValue"[^}]+') status_oldvalue
                                                          , "regexp_extract"(status_str, '"value"[^}]+')    status_value
                                                          , "regexp_extract"(queue_str, '"oldValue"[^}]+')  queue_oldvalue
                                                          , "regexp_extract"(queue_str, '"value"[^}]+')     queue_value
                                                          , status_str
                                                          , analyst_id
                                                          , analyst_name
                                                          , action_time_unix
                                                          , action_time
                                                          , data
                                                     FROM (
                                                              SELECT ticket_id
                                                                   , "regexp_extract"(data, '"field":"Status"[^}]+}')  status_str
                                                                   , "regexp_extract"(data, '"field":"QueueID"[^}]+}') queue_str
                                                                   , creator_id                                        analyst_id
                                                                   , lpu.full_name                                     analyst_name
                                                                   , creation_time                                     action_time_unix
                                                                   , "from_unixtime"(creation_time)                    action_time
                                                                   , data
                                                              FROM ("leon_paycom_transaction:si" lpt
                                                                       LEFT JOIN (
                                                                  SELECT DISTINCT fbid
                                                                                , full_name
                                                                  FROM ("leon_paycom_user:si"
                                                                           INNER JOIN leon_paycom_user_max lpum ON (1 = 1))
                                                                  WHERE (ds = lpum.max_ds)
                                                              ) lpu ON (lpt.creator_id = lpu.fbid))
                                                          )
                                                 ) lpt
                                                     LEFT JOIN (
                                                         SELECT DISTINCT fbid queue_id
                                                                       , name queue_name_from
                                                         FROM ("leon_paycom_queue:si"
                                                                  INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                         WHERE (ds = lpq.max_ds)
                                                     ) qm_old ON (CAST("regexp_extract"(lpt.queue_oldvalue, '[0-9]+') AS bigint) = qm_old.queue_id))
                                                          LEFT JOIN (
                                                     SELECT DISTINCT fbid queue_id
                                                                   , name queue_name_to
                                                     FROM ("leon_paycom_queue:si"
                                                              INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                     WHERE (ds = lpq.max_ds)
                                                 ) qm_new ON (CAST("regexp_extract"(lpt.queue_value, '[0-9]+') AS bigint) = qm_new.queue_id))
                                                 ORDER BY ticket_id ASC, action_time ASC
                                             )
                                        WHERE ((((status_from IS NOT NULL) OR (status_to IS NOT NULL)) OR (queue_name_from IS NOT NULL)) OR
                                               (queue_name_to IS NOT NULL))
                                    )
                           )
                      WHERE ((analyst_name <> 'Leon Paycom Bot') OR (analyst_name IS NULL))
                  ) b
                      INNER JOIN (
                          SELECT (CASE
                                      WHEN (vendor = 0) THEN 'NONE'
                                      WHEN (vendor = 1) THEN 'MANUAL'
                                      WHEN (vendor = 2) THEN 'OTHER'
                                      WHEN (vendor = 3) THEN 'RDC'
                                      WHEN (vendor = 4) THEN 'CSI' END) vendor
                               , ticket_id
                               , inquiry_origin
                               , ds
                          FROM ("staging_paycom_leon_sanction_copy:si"
                                   INNER JOIN staging_paycom_leon_sanction_copy_max splsc_max ON (1 = 1))
                          WHERE (ds = splsc_max.max_ds)
                      ) c ON (b.ticket_id = c.ticket_id))
                           LEFT JOIN (
                      SELECT *
                      FROM (paycom_leon_screening_type_mapping_copy
                               INNER JOIN paycom_leon_screening_type_mapping_copy_max plstm_max ON (1 = 1))
                      WHERE (ds = plstm_max.max_ds)
                  ) d ON (c.inquiry_origin = d.screening_type_enum))
                  ORDER BY b.ticket_id ASC, b.action_time ASC
              )
         WHERE ((((((((((("lower"(status_to_revised) = 'resolved') AND ("lower"(status_from) = 'open')) OR
                        (("lower"(status_to_revised) = 'resolved') AND ("lower"(status_from) = 'duplicate'))) OR
                       (("lower"(status_to_revised) = 'resolved') AND ("lower"(status_from) = 'new'))) OR
                      (("lower"(status_to_revised) = 'escalated') AND (queue_name_from_revised <> queue_name_to_revised))) OR
                     ((("lower"(status_to_revised) = 'open') AND ("lower"(status_from) = 'new')) AND (queue_name_from_revised <> queue_name_to_revised))) OR
                    ((("lower"(status_to_revised) = 'deescalated') AND ("lower"(status_from) = 'escalated')) AND
                     (queue_name_from_revised <> queue_name_to_revised))) OR
                   ((("lower"(status_to_revised) = 'new') AND ("lower"(status_from) = 'open')) AND (queue_name_from_revised <> queue_name_to_revised))) OR
                  (("lower"(status_to_revised) = 'resolved') AND ("lower"(status_from) = 'pending'))) OR
                 (("lower"(status_to_revised) = 'resolved') AND (NOT ("lower"(status_from) IN ('pending', 'duplicate'))))) OR
                (("lower"(status_to_revised) = 'user_wait') AND (NOT ("lower"(status_from) IN ('resolved', 'duplicate', 'user_wait')))))
         UNION ALL
         SELECT action_time                  ds
              , action_time                  action_time
              , queue_name_from_revised      queue_name_to
              , 'auto_close'                 metric_name
              , screening_type_enum_label    screening_type
              , 'Leon'                       system
              , status_to_revised            status_to
              , CAST(a.ticket_id AS varchar) metric_value
              , vendor                       vendor
         FROM ((
                   SELECT b.ticket_id
                        , b.analyst_id
                        , b.analyst_name
                        , b.action_time_unix
                        , b.action_time
                        , b.transaction_data
                        , b.status_from
                        , b.status_to
                        , b.queue_name_from
                        , b.queue_name_to
                        , (CASE
                               WHEN (b.queue_name_to IS NULL)
                                   THEN "first_value"(b.queue_name_to) OVER (PARTITION BY b.ticket_id, b.grp_queue_to ORDER BY b.action_time ASC)
                               ELSE b.queue_name_from END)           queue_name_from_revised
                        , (CASE
                               WHEN (b.queue_name_to IS NULL) THEN "first_value"(b.queue_name_to)
                                                                                 OVER (PARTITION BY b.ticket_id, b.grp_queue_from ORDER BY b.action_time ASC)
                               ELSE b.queue_name_to END)             queue_name_to_revised
                        , (CASE
                               WHEN (b.status_to IS NULL) THEN "first_value"(b.status_to) OVER (PARTITION BY b.ticket_id, b.grp_status ORDER BY b.action_time ASC)
                               ELSE b.status_to END)                 status_to_revised
                        , (CASE
                               WHEN (c.inquiry_origin = 59) THEN 'IG_SHOPPING'
                               WHEN (c.inquiry_origin = 60) THEN 'MACHAMP'
                               WHEN (c.inquiry_origin = 61) THEN 'WA_PAID_MESSAGES'
                               WHEN (c.inquiry_origin = 62) THEN 'IG_P4C'
                               WHEN (c.inquiry_origin = 63) THEN 'IG_P4C_BILLING'
                               WHEN (c.inquiry_origin = 64) THEN 'C2C_COMMERCE'
                               WHEN (c.inquiry_origin = 65) THEN 'GLOBAL_POLITICAL_AUTHORIZATION'
                               WHEN (c.inquiry_origin = 66) THEN 'WA_P2P'
                               WHEN (c.inquiry_origin = 67) THEN 'OCULUS_PORTAL_HARDWARE_STORE'
                               WHEN (c.inquiry_origin = 68) THEN 'IG_P4C_INTL'
                               WHEN (c.inquiry_origin = 69) THEN 'B2C_MESSENGER_COMMERCE'
                               WHEN (c.inquiry_origin = 70) THEN 'VIEWPOINTS'
                               WHEN (c.inquiry_origin = 71) THEN 'GEOBLOCKING'
                               WHEN (c.inquiry_origin = 72) THEN 'VENEZUELA'
                               WHEN (c.inquiry_origin = 73) THEN 'WA_P2M_BRAZIL'
                               ELSE d.screening_type_enum_label END) screening_type_enum_label
                        , c.vendor
                   FROM (((
                       SELECT *
                            , "sum"((CASE WHEN (queue_name_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)   grp_queue_to
                            , "sum"((CASE WHEN (queue_name_from IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC) grp_queue_from
                            , "sum"((CASE WHEN (status_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)       grp_status
                       FROM (
                                SELECT ticket_id
                                     , status_from
                                     , status_to
                                     , queue_id_from
                                     , (CASE
                                            WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                  ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                                THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                            WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                  ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                                THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                            WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                                THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                            WHEN (((status_to = 'OPEN') AND (queue_name_to = 'L1a')) AND (queue_id_from IS NULL)) THEN 'L0'
                                            WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                                THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                            WHEN ((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                                THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                            WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_from IS NULL)) THEN 'L0'
                                            WHEN (((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) THEN 'L0'
                                            WHEN (queue_name_to = 'L0') THEN 'L0'
                                            WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                            WHEN (QUEUE_ID_from = 1651263241863038) THEN 'L0'
                                            ELSE queue_name_from END) queue_name_from
                                     , queue_id_to
                                     , (CASE
                                            WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                  ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                                THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                            WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                  ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                                THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                            WHEN ((status_from = 'NEW') AND (status_to = 'OPEN')) THEN 'L1a'
                                            WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                                THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                            WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_to IS NULL)) THEN 'L1a'
                                            WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                            WHEN (((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                   ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL)) AND
                                                  ("lag"(status_from, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'OPEN'))
                                                THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                            ELSE queue_name_to END)   queue_name_to
                                     , analyst_id
                                     , analyst_name
                                     , action_time_unix
                                     , action_time
                                     , transaction_data
                                FROM (
                                         SELECT ticket_id
                                              , (CASE
                                                     WHEN (status_from IS NULL) THEN "lag"(status_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                                     ELSE status_from END) status_from
                                              , status_to
                                              , queue_id_from
                                              , queue_name_from
                                              , queue_id_to
                                              , queue_name_to
                                              , analyst_id
                                              , analyst_name
                                              , action_time_unix
                                              , action_time
                                              , transaction_data
                                         FROM (
                                                  SELECT ticket_id
                                                       , (CASE
                                                              WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2001') THEN 'NEW'
                                                              WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2002') THEN 'OPEN'
                                                              WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                              WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2004') THEN 'PENDING'
                                                              WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                              WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2006') THEN 'USER_WAIT' END) status_from
                                                       , (CASE
                                                              WHEN ("regexp_extract"(status_value, '[0-9]+') = '2001') THEN 'NEW'
                                                              WHEN ("regexp_extract"(status_value, '[0-9]+') = '2002') THEN 'OPEN'
                                                              WHEN ("regexp_extract"(status_value, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                              WHEN ("regexp_extract"(status_value, '[0-9]+') = '2004') THEN 'PENDING'
                                                              WHEN ("regexp_extract"(status_value, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                              WHEN ("regexp_extract"(status_value, '[0-9]+') = '2006') THEN 'USER_WAIT'
                                                              WHEN (("lower"(queue_name_from) = 'l1a') AND
                                                                    ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                     ("lower"(queue_name_to) = 'ESCALATED'))) THEN 'ESCALATED'
                                                              WHEN (("lower"(queue_name_from) = 'l1b') AND
                                                                    (("lower"(queue_name_to) = 'l2') OR ("lower"(queue_name_to) = 'escalated'))) THEN 'ESCALATED'
                                                              WHEN (("lower"(queue_name_from) = 'l2') AND ("lower"(queue_name_to) = 'escalated')) THEN 'ESCALATED'
                                                              WHEN (("lower"(queue_name_from) = 'escalated') AND
                                                                    ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                     ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                              WHEN (("lower"(queue_name_from) = 'l2') AND
                                                                    (("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                              WHEN (("lower"(queue_name_from) = 'l1b') AND ("lower"(queue_name_to) = 'l1a'))
                                                                  THEN 'DEESCALATED' END)                                                       status_to
                                                       , CAST("regexp_extract"(queue_oldvalue, '[0-9]+') AS bigint)                             queue_id_from
                                                       , queue_name_from
                                                       , CAST("regexp_extract"(queue_value, '[0-9]+') AS bigint)                                queue_id_to
                                                       , queue_name_to
                                                       , analyst_id
                                                       , analyst_name
                                                       , action_time_unix                                                                       action_time_unix
                                                       , action_time                                                                            action_time
                                                       , data                                                                                   transaction_data
                                                  FROM (((
                                                      SELECT ticket_id
                                                           , "regexp_extract"(status_str, '"oldValue"[^}]+') status_oldvalue
                                                           , "regexp_extract"(status_str, '"value"[^}]+')    status_value
                                                           , "regexp_extract"(queue_str, '"oldValue"[^}]+')  queue_oldvalue
                                                           , "regexp_extract"(queue_str, '"value"[^}]+')     queue_value
                                                           , status_str
                                                           , analyst_id
                                                           , analyst_name
                                                           , action_time_unix
                                                           , action_time
                                                           , data
                                                      FROM (
                                                               SELECT ticket_id
                                                                    , "regexp_extract"(data, '"field":"Status"[^}]+}')  status_str
                                                                    , "regexp_extract"(data, '"field":"QueueID"[^}]+}') queue_str
                                                                    , creator_id                                        analyst_id
                                                                    , lpu.full_name                                     analyst_name
                                                                    , creation_time                                     action_time_unix
                                                                    , "from_unixtime"(creation_time)                    action_time
                                                                    , data
                                                               FROM ("leon_paycom_transaction:si" lpt
                                                                        LEFT JOIN (
                                                                   SELECT DISTINCT fbid
                                                                                 , full_name
                                                                   FROM ("leon_paycom_user:si"
                                                                            INNER JOIN leon_paycom_user_max lpum ON (1 = 1))
                                                                   WHERE (ds = lpum.max_ds)
                                                               ) lpu ON (lpt.creator_id = lpu.fbid))
                                                           )
                                                  ) lpt
                                                      LEFT JOIN (
                                                          SELECT DISTINCT fbid queue_id
                                                                        , name queue_name_from
                                                          FROM ("leon_paycom_queue:si"
                                                                   INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                          WHERE (ds = lpq.max_ds)
                                                      ) qm_old ON (CAST("regexp_extract"(lpt.queue_oldvalue, '[0-9]+') AS bigint) = qm_old.queue_id))
                                                           LEFT JOIN (
                                                      SELECT DISTINCT fbid queue_id
                                                                    , name queue_name_to
                                                      FROM ("leon_paycom_queue:si"
                                                               INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                      WHERE (ds = lpq.max_ds)
                                                  ) qm_new ON (CAST("regexp_extract"(lpt.queue_value, '[0-9]+') AS bigint) = qm_new.queue_id))
                                                  ORDER BY ticket_id ASC, action_time ASC
                                              )
                                         WHERE ((((status_from IS NOT NULL) OR (status_to IS NOT NULL)) OR (queue_name_from IS NOT NULL)) OR
                                                (queue_name_to IS NOT NULL))
                                     )
                            )
                       WHERE ((analyst_name = 'Leon Paycom Bot') OR (analyst_name IS NULL))
                   ) b
                       INNER JOIN (
                           SELECT (CASE
                                       WHEN (vendor = 0) THEN 'NONE'
                                       WHEN (vendor = 1) THEN 'MANUAL'
                                       WHEN (vendor = 2) THEN 'OTHER'
                                       WHEN (vendor = 3) THEN 'RDC'
                                       WHEN (vendor = 4) THEN 'CSI' END) vendor
                                , ticket_id
                                , inquiry_origin
                                , ds
                           FROM ("staging_paycom_leon_sanction_copy:si"
                                    INNER JOIN staging_paycom_leon_sanction_copy_max splsc_max ON (1 = 1))
                           WHERE (ds = splsc_max.max_ds)
                       ) c ON (b.ticket_id = c.ticket_id))
                            LEFT JOIN (
                       SELECT *
                       FROM (paycom_leon_screening_type_mapping_copy
                                INNER JOIN paycom_leon_screening_type_mapping_copy_max plstm_max ON (1 = 1))
                       WHERE (ds = plstm_max.max_ds)
                   ) d ON (c.inquiry_origin = d.screening_type_enum))
                   ORDER BY b.ticket_id ASC, b.action_time ASC
               ) a
                  LEFT JOIN (
             SELECT ticketfbid ticket_id
                  , tagfbid
             FROM ("leon_paycom_ticket_tag:si"
                      INNER JOIN leon_paycom_ticket_tag_max lptt_max ON (1 = 1))
             WHERE ((ds = lptt_max.max_ds) AND (tagfbid IN (413240162764669, 486429145453155, 2461016614182101, 429911514401974)))
         ) b ON (b.ticket_id = a.ticket_id))
         WHERE ((("lower"(status_to_revised) IN ('resolved')) AND (NOT ("lower"(status_from) IN ('user_wait', 'duplicate')))) AND
                ((analyst_name IS NULL) OR (analyst_name = 'Leon Paycom Bot')))
         UNION
         SELECT action_time                ds
              , action_time                action_time
              , queue_name_from_revised    queue_name_to
              , 'manual_close'             metric_name
              , screening_type_enum_label  screening_type
              , 'Leon'                     system
              , status_to_revised          status_to
              , CAST(ticket_id AS varchar) metric_value
              , vendor                     vendor
         FROM (
                  SELECT b.ticket_id
                       , b.analyst_id
                       , b.analyst_name
                       , b.action_time_unix
                       , b.action_time
                       , b.transaction_data
                       , b.status_from
                       , b.status_to
                       , b.queue_name_from
                       , b.queue_name_to
                       , (CASE
                              WHEN (b.queue_name_to IS NULL)
                                  THEN "first_value"(b.queue_name_to) OVER (PARTITION BY b.ticket_id, b.grp_queue_to ORDER BY b.action_time ASC)
                              ELSE b.queue_name_from END)           queue_name_from_revised
                       , (CASE
                              WHEN (b.queue_name_to IS NULL) THEN "first_value"(b.queue_name_to)
                                                                                OVER (PARTITION BY b.ticket_id, b.grp_queue_from ORDER BY b.action_time ASC)
                              ELSE b.queue_name_to END)             queue_name_to_revised
                       , (CASE
                              WHEN (b.status_to IS NULL) THEN "first_value"(b.status_to) OVER (PARTITION BY b.ticket_id, b.grp_status ORDER BY b.action_time ASC)
                              ELSE b.status_to END)                 status_to_revised
                       , (CASE
                              WHEN (c.inquiry_origin = 59) THEN 'IG_SHOPPING'
                              WHEN (c.inquiry_origin = 60) THEN 'MACHAMP'
                              WHEN (c.inquiry_origin = 61) THEN 'WA_PAID_MESSAGES'
                              WHEN (c.inquiry_origin = 62) THEN 'IG_P4C'
                              WHEN (c.inquiry_origin = 63) THEN 'IG_P4C_BILLING'
                              WHEN (c.inquiry_origin = 64) THEN 'C2C_COMMERCE'
                              WHEN (c.inquiry_origin = 65) THEN 'GLOBAL_POLITICAL_AUTHORIZATION'
                              WHEN (c.inquiry_origin = 66) THEN 'WA_P2P'
                              WHEN (c.inquiry_origin = 67) THEN 'OCULUS_PORTAL_HARDWARE_STORE'
                              WHEN (c.inquiry_origin = 68) THEN 'IG_P4C_INTL'
                              WHEN (c.inquiry_origin = 69) THEN 'B2C_MESSENGER_COMMERCE'
                              WHEN (c.inquiry_origin = 70) THEN 'VIEWPOINTS'
                              WHEN (c.inquiry_origin = 71) THEN 'GEOBLOCKING'
                              WHEN (c.inquiry_origin = 72) THEN 'VENEZUELA'
                              WHEN (c.inquiry_origin = 73) THEN 'WA_P2M_BRAZIL'
                              ELSE d.screening_type_enum_label END) screening_type_enum_label
                       , c.vendor
                  FROM (((
                      SELECT *
                           , "sum"((CASE WHEN (queue_name_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)   grp_queue_to
                           , "sum"((CASE WHEN (queue_name_from IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC) grp_queue_from
                           , "sum"((CASE WHEN (status_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)       grp_status
                      FROM (
                               SELECT ticket_id
                                    , status_from
                                    , status_to
                                    , queue_id_from
                                    , (CASE
                                           WHEN ((((status_from = 'DUPLICATE') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_to = 'OPEN') AND (queue_name_to = 'L1a')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (queue_name_to = 'L0') THEN 'L0'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (QUEUE_ID_from = 1651263241863038) THEN 'L0'
                                           ELSE queue_name_from END) queue_name_from
                                    , queue_id_to
                                    , (CASE
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'DUPLICATE') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'RESOLVED') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'OPEN') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "first_value"(queue_name_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((status_from = 'NEW') AND (status_to = 'OPEN')) THEN 'L1a'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_to IS NULL)) THEN 'L1a'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL)) AND
                                                 ("lag"(status_from, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'OPEN'))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           ELSE queue_name_to END)   queue_name_to
                                    , analyst_id
                                    , analyst_name
                                    , action_time_unix
                                    , action_time
                                    , transaction_data
                               FROM (
                                        SELECT ticket_id
                                             , (CASE
                                                    WHEN (status_from IS NULL) THEN "lag"(status_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                                    ELSE status_from END) status_from
                                             , status_to
                                             , queue_id_from
                                             , queue_name_from
                                             , queue_id_to
                                             , queue_name_to
                                             , analyst_id
                                             , analyst_name
                                             , action_time_unix
                                             , action_time
                                             , transaction_data
                                        FROM (
                                                 SELECT ticket_id
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2006') THEN 'USER_WAIT' END) status_from
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2006') THEN 'USER_WAIT'
                                                             WHEN (("lower"(queue_name_from) = 'l1a') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'ESCALATED'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND
                                                                   (("lower"(queue_name_to) = 'l2') OR ("lower"(queue_name_to) = 'escalated'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND ("lower"(queue_name_to) = 'escalated')) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'escalated') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND
                                                                   (("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND ("lower"(queue_name_to) = 'l1a'))
                                                                 THEN 'DEESCALATED' END)                                                       status_to
                                                      , CAST("regexp_extract"(queue_oldvalue, '[0-9]+') AS bigint)                             queue_id_from
                                                      , queue_name_from
                                                      , CAST("regexp_extract"(queue_value, '[0-9]+') AS bigint)                                queue_id_to
                                                      , queue_name_to
                                                      , analyst_id
                                                      , analyst_name
                                                      , action_time_unix                                                                       action_time_unix
                                                      , action_time                                                                            action_time
                                                      , data                                                                                   transaction_data
                                                 FROM (((
                                                     SELECT ticket_id
                                                          , "regexp_extract"(status_str, '"oldValue"[^}]+') status_oldvalue
                                                          , "regexp_extract"(status_str, '"value"[^}]+')    status_value
                                                          , "regexp_extract"(queue_str, '"oldValue"[^}]+')  queue_oldvalue
                                                          , "regexp_extract"(queue_str, '"value"[^}]+')     queue_value
                                                          , status_str
                                                          , analyst_id
                                                          , analyst_name
                                                          , action_time_unix
                                                          , action_time
                                                          , data
                                                     FROM (
                                                              SELECT ticket_id
                                                                   , "regexp_extract"(data, '"field":"Status"[^}]+}')  status_str
                                                                   , "regexp_extract"(data, '"field":"QueueID"[^}]+}') queue_str
                                                                   , creator_id                                        analyst_id
                                                                   , lpu.full_name                                     analyst_name
                                                                   , creation_time                                     action_time_unix
                                                                   , "from_unixtime"(creation_time)                    action_time
                                                                   , data
                                                              FROM ("leon_paycom_transaction:si" lpt
                                                                       LEFT JOIN (
                                                                  SELECT DISTINCT fbid
                                                                                , full_name
                                                                  FROM ("leon_paycom_user:si"
                                                                           INNER JOIN leon_paycom_user_max lpum ON (1 = 1))
                                                                  WHERE (ds = lpum.max_ds)
                                                              ) lpu ON (lpt.creator_id = lpu.fbid))
                                                          )
                                                 ) lpt
                                                     LEFT JOIN (
                                                         SELECT DISTINCT fbid queue_id
                                                                       , name queue_name_from
                                                         FROM ("leon_paycom_queue:si"
                                                                  INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                         WHERE (ds = lpq.max_ds)
                                                     ) qm_old ON (CAST("regexp_extract"(lpt.queue_oldvalue, '[0-9]+') AS bigint) = qm_old.queue_id))
                                                          LEFT JOIN (
                                                     SELECT DISTINCT fbid queue_id
                                                                   , name queue_name_to
                                                     FROM ("leon_paycom_queue:si"
                                                              INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                     WHERE (ds = lpq.max_ds)
                                                 ) qm_new ON (CAST("regexp_extract"(lpt.queue_value, '[0-9]+') AS bigint) = qm_new.queue_id))
                                                 ORDER BY ticket_id ASC, action_time ASC
                                             )
                                        WHERE ((((status_from IS NOT NULL) OR (status_to IS NOT NULL)) OR (queue_name_from IS NOT NULL)) OR
                                               (queue_name_to IS NOT NULL))
                                    )
                           )
                      WHERE ((analyst_name <> 'Leon Paycom Bot') OR (analyst_name IS NOT NULL))
                  ) b
                      INNER JOIN (
                          SELECT (CASE
                                      WHEN (vendor = 0) THEN 'NONE'
                                      WHEN (vendor = 1) THEN 'MANUAL'
                                      WHEN (vendor = 2) THEN 'OTHER'
                                      WHEN (vendor = 3) THEN 'RDC'
                                      WHEN (vendor = 4) THEN 'CSI' END) vendor
                               , ticket_id
                               , inquiry_origin
                               , ds
                          FROM ("staging_paycom_leon_sanction_copy:si"
                                   INNER JOIN staging_paycom_leon_sanction_copy_max splsc_max ON (1 = 1))
                          WHERE (ds = splsc_max.max_ds)
                      ) c ON (b.ticket_id = c.ticket_id))
                           LEFT JOIN (
                      SELECT *
                      FROM (paycom_leon_screening_type_mapping_copy
                               INNER JOIN paycom_leon_screening_type_mapping_copy_max plstm_max ON (1 = 1))
                      WHERE (ds = plstm_max.max_ds)
                  ) d ON (c.inquiry_origin = d.screening_type_enum))
                  ORDER BY b.ticket_id ASC, b.action_time ASC
              )
         WHERE ((("lower"(status_to_revised) = 'resolved') AND (NOT ("lower"(status_from) IN ('resolved')))) AND (queue_name_from_revised = queue_name_to_revised))
         UNION ALL
         SELECT action_time                ds
              , action_time                action_time
              , queue_name_to_revised      queue_name_to
              , 'manual_close'             metric_name
              , screening_type_enum_label  screening_type
              , 'Leon'                     system
              , status_to_revised          status_to
              , CAST(ticket_id AS varchar) metric_value
              , vendor                     vendor
         FROM (
                  SELECT b.ticket_id
                       , b.analyst_id
                       , b.analyst_name
                       , b.action_time_unix
                       , b.action_time
                       , b.transaction_data
                       , b.status_from
                       , b.status_to
                       , b.queue_name_from
                       , b.queue_name_to
                       , (CASE
                              WHEN (b.queue_name_to IS NULL)
                                  THEN "first_value"(b.queue_name_to) OVER (PARTITION BY b.ticket_id, b.grp_queue_to ORDER BY b.action_time ASC)
                              ELSE b.queue_name_from END)           queue_name_from_revised
                       , (CASE
                              WHEN (b.queue_name_to IS NULL) THEN "first_value"(b.queue_name_to)
                                                                                OVER (PARTITION BY b.ticket_id, b.grp_queue_from ORDER BY b.action_time ASC)
                              ELSE b.queue_name_to END)             queue_name_to_revised
                       , (CASE
                              WHEN (b.status_to IS NULL) THEN "first_value"(b.status_to) OVER (PARTITION BY b.ticket_id, b.grp_status ORDER BY b.action_time ASC)
                              ELSE b.status_to END)                 status_to_revised
                       , (CASE
                              WHEN (c.inquiry_origin = 59) THEN 'IG_SHOPPING'
                              WHEN (c.inquiry_origin = 60) THEN 'MACHAMP'
                              WHEN (c.inquiry_origin = 61) THEN 'WA_PAID_MESSAGES'
                              WHEN (c.inquiry_origin = 62) THEN 'IG_P4C'
                              WHEN (c.inquiry_origin = 63) THEN 'IG_P4C_BILLING'
                              WHEN (c.inquiry_origin = 64) THEN 'C2C_COMMERCE'
                              WHEN (c.inquiry_origin = 65) THEN 'GLOBAL_POLITICAL_AUTHORIZATION'
                              WHEN (c.inquiry_origin = 66) THEN 'WA_P2P'
                              WHEN (c.inquiry_origin = 67) THEN 'OCULUS_PORTAL_HARDWARE_STORE'
                              WHEN (c.inquiry_origin = 68) THEN 'IG_P4C_INTL'
                              WHEN (c.inquiry_origin = 69) THEN 'B2C_MESSENGER_COMMERCE'
                              WHEN (c.inquiry_origin = 70) THEN 'VIEWPOINTS'
                              WHEN (c.inquiry_origin = 71) THEN 'GEOBLOCKING'
                              WHEN (c.inquiry_origin = 72) THEN 'VENEZUELA'
                              WHEN (c.inquiry_origin = 73) THEN 'WA_P2M_BRAZIL'
                              ELSE d.screening_type_enum_label END) screening_type_enum_label
                       , c.vendor
                  FROM (((
                      SELECT *
                           , "sum"((CASE WHEN (queue_name_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)   grp_queue_to
                           , "sum"((CASE WHEN (queue_name_from IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC) grp_queue_from
                           , "sum"((CASE WHEN (status_to IS NOT NULL) THEN 1 END)) OVER (PARTITION BY ticket_id ORDER BY action_time ASC)       grp_status
                      FROM (
                               SELECT ticket_id
                                    , status_from
                                    , status_to
                                    , queue_id_from
                                    , (CASE
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'OPEN')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_to = 'OPEN') AND (queue_name_to = 'L1a')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN ((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_from IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (((status_from = 'NEW') AND (status_to = 'DUPLICATE')) AND (queue_id_from IS NULL)) THEN 'L0'
                                           WHEN (queue_name_to = 'L0') THEN 'L0'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (QUEUE_ID_from = 1651263241863038) THEN 'L0'
                                           ELSE queue_name_from END)                                                  queue_name_from
                                    , queue_id_to
                                    , (CASE
                                           WHEN ((status_from = 'NEW') AND (status_to = 'OPEN')) THEN 'L1a'
                                           WHEN ((((status_from = 'NEW') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                 ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           WHEN (((status_from = 'NEW') AND (status_to = 'PENDING')) AND (queue_id_to IS NULL)) THEN 'L1a'
                                           WHEN (((status_from IS NULL) AND (status_to = 'NEW')) AND (QUEUE_ID_to = 1651263241863038)) THEN 'L0'
                                           WHEN (((((status_from = 'PENDING') AND (status_to = 'RESOLVED')) AND (queue_id_to IS NULL)) AND
                                                  ("lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) IS NOT NULL)) AND
                                                 ("lag"(status_from, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) = 'OPEN'))
                                               THEN "lag"(queue_name_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                           ELSE queue_name_to END)                                                    queue_name_to
                                    , analyst_id
                                    , analyst_name
                                    , action_time_unix
                                    , action_time
                                    , transaction_data
                                    , "lag"(status_to, 1) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC) lag_status
                               FROM (
                                        SELECT ticket_id
                                             , (CASE
                                                    WHEN (status_from IS NULL) THEN "lag"(status_to) OVER (PARTITION BY ticket_id ORDER BY action_time_unix ASC)
                                                    ELSE status_from END) status_from
                                             , status_to
                                             , queue_id_from
                                             , queue_name_from
                                             , queue_id_to
                                             , queue_name_to
                                             , analyst_id
                                             , analyst_name
                                             , action_time_unix
                                             , action_time
                                             , transaction_data
                                        FROM (
                                                 SELECT ticket_id
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_oldValue, '[0-9]+') = '2006') THEN 'USER_WAIT' END) status_from
                                                      , (CASE
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2001') THEN 'NEW'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2002') THEN 'OPEN'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2003') THEN 'RESOLVED'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2004') THEN 'PENDING'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2005') THEN 'DUPLICATE'
                                                             WHEN ("regexp_extract"(status_value, '[0-9]+') = '2006') THEN 'USER_WAIT'
                                                             WHEN (("lower"(queue_name_from) = 'l1a') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'ESCALATED'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND
                                                                   (("lower"(queue_name_to) = 'l2') OR ("lower"(queue_name_to) = 'escalated'))) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND ("lower"(queue_name_to) = 'escalated')) THEN 'ESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'escalated') AND
                                                                   ((("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l2')) OR
                                                                    ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l2') AND
                                                                   (("lower"(queue_name_to) = 'l1b') OR ("lower"(queue_name_to) = 'l1a'))) THEN 'DEESCALATED'
                                                             WHEN (("lower"(queue_name_from) = 'l1b') AND ("lower"(queue_name_to) = 'l1a'))
                                                                 THEN 'DEESCALATED' END)                                                       status_to
                                                      , CAST("regexp_extract"(queue_oldvalue, '[0-9]+') AS bigint)                             queue_id_from
                                                      , queue_name_from
                                                      , CAST("regexp_extract"(queue_value, '[0-9]+') AS bigint)                                queue_id_to
                                                      , queue_name_to
                                                      , analyst_id
                                                      , analyst_name
                                                      , action_time_unix                                                                       action_time_unix
                                                      , action_time                                                                            action_time
                                                      , data                                                                                   transaction_data
                                                 FROM (((
                                                     SELECT ticket_id
                                                          , "regexp_extract"(status_str, '"oldValue"[^}]+') status_oldvalue
                                                          , "regexp_extract"(status_str, '"value"[^}]+')    status_value
                                                          , "regexp_extract"(queue_str, '"oldValue"[^}]+')  queue_oldvalue
                                                          , "regexp_extract"(queue_str, '"value"[^}]+')     queue_value
                                                          , status_str
                                                          , analyst_id
                                                          , analyst_name
                                                          , action_time_unix
                                                          , action_time
                                                          , data
                                                     FROM (
                                                              SELECT ticket_id
                                                                   , "regexp_extract"(data, '"field":"Status"[^}]+}')  status_str
                                                                   , "regexp_extract"(data, '"field":"QueueID"[^}]+}') queue_str
                                                                   , creator_id                                        analyst_id
                                                                   , lpu.full_name                                     analyst_name
                                                                   , creation_time                                     action_time_unix
                                                                   , "from_unixtime"(creation_time)                    action_time
                                                                   , data
                                                              FROM ("leon_paycom_transaction:si" lpt
                                                                       LEFT JOIN (
                                                                  SELECT DISTINCT fbid
                                                                                , full_name
                                                                  FROM ("leon_paycom_user:si"
                                                                           INNER JOIN leon_paycom_user_max lpum ON (1 = 1))
                                                                  WHERE (ds = lpum.max_ds)
                                                              ) lpu ON (lpt.creator_id = lpu.fbid))
                                                          )
                                                 ) lpt
                                                     LEFT JOIN (
                                                         SELECT DISTINCT fbid queue_id
                                                                       , name queue_name_from
                                                         FROM ("leon_paycom_queue:si"
                                                                  INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                         WHERE (ds = lpq.max_ds)
                                                     ) qm_old ON (CAST("regexp_extract"(lpt.queue_oldvalue, '[0-9]+') AS bigint) = qm_old.queue_id))
                                                          LEFT JOIN (
                                                     SELECT DISTINCT fbid queue_id
                                                                   , name queue_name_to
                                                     FROM ("leon_paycom_queue:si"
                                                              INNER JOIN leon_paycom_queue_max lpq ON (1 = 1))
                                                     WHERE (ds = lpq.max_ds)
                                                 ) qm_new ON (CAST("regexp_extract"(lpt.queue_value, '[0-9]+') AS bigint) = qm_new.queue_id))
                                                 ORDER BY ticket_id ASC, action_time ASC
                                             )
                                        WHERE ((((status_from IS NOT NULL) OR (status_to IS NOT NULL)) OR (queue_name_from IS NOT NULL)) OR
                                               (queue_name_to IS NOT NULL))
                                    )
                           )
                      WHERE ((analyst_name <> 'Leon Paycom Bot') OR (analyst_name IS NOT NULL))
                  ) b
                      INNER JOIN (
                          SELECT (CASE
                                      WHEN (vendor = 0) THEN 'NONE'
                                      WHEN (vendor = 1) THEN 'MANUAL'
                                      WHEN (vendor = 2) THEN 'OTHER'
                                      WHEN (vendor = 3) THEN 'RDC'
                                      WHEN (vendor = 4) THEN 'CSI' END) vendor
                               , ticket_id
                               , inquiry_origin
                               , ds
                          FROM ("staging_paycom_leon_sanction_copy:si"
                                   INNER JOIN staging_paycom_leon_sanction_copy_max splsc_max ON (1 = 1))
                          WHERE (ds = splsc_max.max_ds)
                      ) c ON (b.ticket_id = c.ticket_id))
                           LEFT JOIN (
                      SELECT *
                      FROM (paycom_leon_screening_type_mapping_copy
                               INNER JOIN paycom_leon_screening_type_mapping_copy_max plstm_max ON (1 = 1))
                      WHERE (ds = plstm_max.max_ds)
                  ) d ON (c.inquiry_origin = d.screening_type_enum))
                  WHERE ((("lower"(b.status_to) = 'resolved') AND (b.lag_status <> "lower"(b.status_to))) AND (b.queue_name_from <> b.queue_name_to))
                  ORDER BY b.ticket_id ASC, b.action_time ASC
              )
     )
WHERE (ds IS NOT NULL)
UNION ALL
SELECT CAST("date"("min"(action_time)) AS varchar) ds
     , "min"(action_time)                          action_time
     , 'L0'                                        queue_name_to
     , 'Inflow'                                    metric_name
     , screening_type
     , 'CSI Watchdog'                              system
     , 'Created'                                   status_to
     , metric_value
     , 'CSI'                                       vendor
FROM (
         SELECT "min"((CASE
                           WHEN ("length"(transaction_date) > 17) THEN "date_parse"(transaction_date, '%Y-%m-%d %H:%i:%s.%f')
                           WHEN ("length"(transaction_date) < 17) THEN "date_parse"(transaction_date, '%m/%d/%y %H:%i') END)) action_time
              , product_division                                                                                              screening_type
              , (CASE WHEN ((customer_id = '') OR (customer_id = 'N/A')) THEN transaction_id ELSE customer_id END)            metric_value
         FROM csi_watchdog_txn_list_20170101_20191021
         WHERE (product_division IN ('GTM', 'P4C_US', 'P4C_INTL', 'Oculus'))
         GROUP BY 2, 3
         UNION ALL
         SELECT "min"("date"("date_parse"(date, '%m/%d/%Y')))                                               action_time
              , division                                                                                    screening_type
              , (CASE WHEN ((customer_id = '') OR (customer_id = 'N/A')) THEN tran_id ELSE customer_id END) metric_value
         FROM "PAYCOM_CSI_WATCHDOG_TXN_LIST_DAILY"
         WHERE (division IN ('GTM', 'P4C_US', 'P4C_INTL', 'Oculus', 'Building 8'))
         GROUP BY 2, 3
     )
GROUP BY screening_type, metric_value
UNION ALL
SELECT CAST("date"("min"(action_time)) AS varchar) ds
     , "min"(action_time)                          action_time
     , 'L0'                                        queue_name_to
     , 'Outflow'                                   metric_name
     , screening_type
     , 'CSI Watchdog'                              system
     , 'Closed'                                    status_to
     , metric_value
     , 'CSI'                                       vendor
FROM (
         SELECT "min"("date_parse"(date_time_last_modified, '%Y-%m-%d %H:%i:%s.%f'))                               action_time
              , business_line                                                                                      screening_type
              , (CASE WHEN ((customer_id = '') OR (customer_id = 'N/A')) THEN transaction_id ELSE customer_id END) metric_value
         FROM csi_watchdog_txn_activity_20170101_20191021
         WHERE (((date_time_last_modified <> '') AND (business_line IN ('GTM', 'P4C_US', 'P4C_INTL', 'Oculus'))) AND
                (((overall_transaction_status = 'GCL') OR (overall_transaction_status = 'BCL')) OR (overall_transaction_status = 'Reviewed')))
         GROUP BY 2, 3
         UNION ALL
         SELECT "min"("date_parse"(date_time_last_modified, '%m/%d/%Y %H:%i %p'))                                               action_time
              , division                                                                                                        screening_type
              , (CASE WHEN ((customer_id = '') OR (customer_id = 'N/A')) THEN "substr"(transaction_id, 2) ELSE customer_id END) metric_value
         FROM "PAYCOM_CSI_WATCHDOG_TXN_ACT_DAILY"
         WHERE ((division IN ('GTM', 'P4C_US', 'P4C_INTL', 'Oculus', 'Building 8')) AND
                (((overall_transaction_status = 'GCL') OR (overall_transaction_status = 'BCL')) OR (overall_transaction_status = 'Reviewed')))
         GROUP BY 2, 3
     )
GROUP BY screening_type, metric_value
UNION ALL
SELECT CAST("date"("min"(action_time)) AS varchar) ds
     , "min"(action_time)                          action_time
     , 'L0'                                        queue_name_to
     , 'manual_close'                              metric_name
     , screening_type
     , 'CSI Watchdog'                              system
     , 'Closed'                                    status_to
     , metric_value
     , 'CSI'                                       vendor
FROM (
         SELECT "min"("date_parse"(date_time_last_modified, '%Y-%m-%d %H:%i:%s.%f'))                               action_time
              , business_line                                                                                      screening_type
              , (CASE WHEN ((customer_id = '') OR (customer_id = 'N/A')) THEN transaction_id ELSE customer_id END) metric_value
         FROM csi_watchdog_txn_activity_20170101_20191021
         WHERE (((date_time_last_modified <> '') AND (business_line IN ('GTM', 'P4C_US', 'P4C_INTL', 'Oculus'))) AND
                (((overall_transaction_status = 'GCL') OR (overall_transaction_status = 'BCL')) OR (overall_transaction_status = 'Reviewed')))
         GROUP BY 2, 3
         UNION ALL
         SELECT "min"("date_parse"(date_time_last_modified, '%m/%d/%Y %H:%i %p'))                                               action_time
              , division                                                                                                        screening_type
              , (CASE WHEN ((customer_id = '') OR (customer_id = 'N/A')) THEN "substr"(transaction_id, 2) ELSE customer_id END) metric_value
         FROM "PAYCOM_CSI_WATCHDOG_TXN_ACT_DAILY"
         WHERE ((division IN ('GTM', 'P4C_US', 'P4C_INTL', 'Oculus', 'Building 8')) AND
                (((overall_transaction_status = 'GCL') OR (overall_transaction_status = 'BCL')) OR (overall_transaction_status = 'Reviewed')))
         GROUP BY 2, 3
     )
GROUP BY screening_type, metric_value
