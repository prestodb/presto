-- icm_pch_forecast_status
CREATE VIEW prism.bizapps.icm_pch_forecast_status AS
    (
        SELECT site                                                                                                              forecast_site
             , "sum"(IF((type = 'Budget'), sum_amountthisperiod, 0))                                                             "forecast_budget"
             , "sum"(IF((type = 'Forecast'), sum_amountthisperiod, 0))                                                           "forecast_forecast"
             , "sum"(IF((type = 'Actual'), sum_amountthisperiod, 0))                                                             "forecast_actuals"
             , ("sum"(IF((type = 'Forecast'), sum_amountthisperiod, 0)) - "sum"(IF((type = 'Budget'), sum_amountthisperiod, 0))) "forecast_delta$"
             , (("sum"(IF((type = 'Forecast'), sum_amountthisperiod, 0)) - "sum"(IF((type = 'Budget'), sum_amountthisperiod, 0))) /
                "sum"(IF((type = 'Budget'), sum_amountthisperiod, 0)))                                                           "forecast_delta%"
             , IF((locktype = 'Lock'), "concat"('https://tableau.thefacebook.com/#/views/TESTING-ICMCostVariance-SQLDataSource/ForecastVarianceLock?&site=', site),
                  "concat"('https://tableau.thefacebook.com/#/views/ICMForecastVarianceLive/ForecastVarianceLive?&site=', site)) "forecast_url"
             , "max"(viewds)                                                                                                     "forecast_ds"
             , locktype                                                                                                          forecast_locktype
        FROM (
                 SELECT IF((forecast_month = 'Actual'), 'Actual', "concat"(forecast_month_year, '-', forecast_month_number)) forecast_month_year_month
                      , *
                 FROM (
                          SELECT *
                               , "substr"(forecast_month, 5, 4) forecast_month_year
                               , (CASE
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Jan') THEN '01'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Feb') THEN '02'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Mar') THEN '03'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Apr') THEN '04'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'May') THEN '05'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Jun') THEN '06'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Jul') THEN '07'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Aug') THEN '08'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Sep') THEN '09'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Oct') THEN '10'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Nov') THEN '11'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Dec') THEN '12'
                                      ELSE forecast_month END)  forecast_month_number
                          FROM idc_icm_cost_reporting
                          WHERE (((ds = (SELECT "max"(ds)
                                         FROM idc_icm_cost_reporting
                          )) AND (project_months >= (SELECT (CASE
                                                                 WHEN ("substr"("date_format"(lock_month, '%Y-%m-%d'), 6, 2) IN ('01', '04', '07', '10'))
                                                                     THEN (SELECT "date_add"('quarter', -1, "date_trunc"('quarter', "date_parse"("max"(forecast_date), '%Y-%m-%d')))
                                                                           FROM icm_cost_ctrl_cost_tblu
                                                                           WHERE (ds = (SELECT "max"(ds)
                                                                                        FROM icm_cost_ctrl_cost_tblu
                                                                           ))
                                                                 )
                                                                 ELSE (SELECT "date_trunc"('quarter', "date_parse"("max"(forecast_date), '%Y-%m-%d')) lock_month
                                                                       FROM icm_cost_ctrl_cost_tblu
                                                                       WHERE (ds = (SELECT "max"(ds)
                                                                                    FROM icm_cost_ctrl_cost_tblu
                                                                       ))
                                                                 ) END)
                                                     FROM (
                                                              SELECT "date_trunc"('month', "date_parse"("max"(forecast_date), '%Y-%m-%d')) lock_month
                                                              FROM icm_cost_ctrl_cost_tblu
                                                              WHERE (ds = (SELECT "max"(ds)
                                                                           FROM icm_cost_ctrl_cost_tblu
                                                              ))
                                                          )
                          ))) AND (project_months < (SELECT (CASE
                                                                 WHEN ("substr"("date_format"(lock_month, '%Y-%m-%d'), 6, 2) IN ('01', '04', '07', '10'))
                                                                     THEN (SELECT "date_trunc"('quarter', "date_parse"("max"(forecast_date), '%Y-%m-%d')) lock_month
                                                                           FROM icm_cost_ctrl_cost_tblu
                                                                           WHERE (ds = (SELECT "max"(ds)
                                                                                        FROM icm_cost_ctrl_cost_tblu
                                                                           ))
                                                                 )
                                                                 ELSE (SELECT "date_add"('quarter', 1, "date_trunc"('quarter', "date_parse"("max"(forecast_date), '%Y-%m-%d')))
                                                                       FROM icm_cost_ctrl_cost_tblu
                                                                       WHERE (ds = (SELECT "max"(ds)
                                                                                    FROM icm_cost_ctrl_cost_tblu
                                                                       ))
                                                                 ) END)
                                                     FROM (
                                                              SELECT "date_trunc"('month', "date_parse"("max"(forecast_date), '%Y-%m-%d')) lock_month
                                                              FROM icm_cost_ctrl_cost_tblu
                                                              WHERE (ds = (SELECT "max"(ds)
                                                                           FROM icm_cost_ctrl_cost_tblu
                                                              ))
                                                          )
                          )))
                      )
             )
        WHERE ((((((forecast_month_year_month = (SELECT (CASE
                                                             WHEN ("substr"("date_format"(lock_month, '%Y-%m-%d'), 6, 2) IN ('01', '04', '07', '10')) THEN (SELECT "date_format"(
                                                                                                                                                                     "date_add"(
                                                                                                                                                                       'month', 2,
                                                                                                                                                                       "date_add"('quarter', -2, "date_trunc"('quarter', "date_parse"("max"(forecast_date), '%Y-%m-%d')))),
                                                                                                                                                                     '%Y-%m')
                                                                                                                                                            FROM icm_cost_ctrl_cost_tblu
                                                                                                                                                            WHERE (ds =
                                                                                                                                                                   (SELECT "max"(ds)
                                                                                                                                                                    FROM icm_cost_ctrl_cost_tblu
                                                                                                                                                                   ))
                                                             )
                                                             ELSE (SELECT "date_format"("date_add"('month', 2,
                                                                                                   "date_add"('quarter', -1, "date_trunc"('quarter', "date_parse"("max"(forecast_date), '%Y-%m-%d')))),
                                                                                        '%Y-%m')
                                                                   FROM icm_cost_ctrl_cost_tblu
                                                                   WHERE (ds = (SELECT "max"(ds)
                                                                                FROM icm_cost_ctrl_cost_tblu
                                                                   ))
                                                             ) END)
                                                 FROM (
                                                          SELECT "date_trunc"('month', "date_parse"("max"(forecast_date), '%Y-%m-%d')) lock_month
                                                          FROM icm_cost_ctrl_cost_tblu
                                                          WHERE (ds = (SELECT "max"(ds)
                                                                       FROM icm_cost_ctrl_cost_tblu
                                                          ))
                                                      )
        )) AND (type = 'Budget')) OR (type IN ('Actual', 'Forecast'))) AND (project_owner = 'New Builds')) AND (status IN ('Active', 'Planning'))) AND (locktype = 'Lock'))
        GROUP BY site, locktype
        ORDER BY site ASC
    )
    UNION ALL
    (
        SELECT site                                                                                                              forecast_site
             , "sum"(IF((type = 'Budget'), sum_amountthisperiod, 0))                                                             "forecast_budget"
             , "sum"(IF((type = 'Forecast'), sum_amountthisperiod, 0))                                                           "forecast_forecast"
             , "sum"(IF((type = 'Actual'), sum_amountthisperiod, 0))                                                             "forecast_actuals"
             , ("sum"(IF((type = 'Forecast'), sum_amountthisperiod, 0)) - "sum"(IF((type = 'Budget'), sum_amountthisperiod, 0))) "forecast_delta$"
             , (("sum"(IF((type = 'Forecast'), sum_amountthisperiod, 0)) - "sum"(IF((type = 'Budget'), sum_amountthisperiod, 0))) /
                "sum"(IF((type = 'Budget'), sum_amountthisperiod, 0)))                                                           "forecast_delta%"
             , IF((locktype = 'Lock'), "concat"('https://tableau.thefacebook.com/#/views/TESTING-ICMCostVariance-SQLDataSource/ForecastVarianceLock?&site=', site),
                  "concat"('https://tableau.thefacebook.com/#/views/ICMForecastVarianceLive/ForecastVarianceLive?&site=', site)) "forecast_url"
             , "max"(viewds)                                                                                                     "forecast_ds"
             , locktype                                                                                                          forecast_locktype
        FROM (
                 SELECT IF((forecast_month = 'Actual'), 'Actual', "concat"(forecast_month_year, '-', forecast_month_number)) forecast_month_year_month
                      , *
                 FROM (
                          SELECT *
                               , "substr"(forecast_month, 5, 4) forecast_month_year
                               , (CASE
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Jan') THEN '01'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Feb') THEN '02'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Mar') THEN '03'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Apr') THEN '04'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'May') THEN '05'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Jun') THEN '06'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Jul') THEN '07'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Aug') THEN '08'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Sep') THEN '09'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Oct') THEN '10'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Nov') THEN '11'
                                      WHEN ("substr"(forecast_month, 1, 3) = 'Dec') THEN '12'
                                      ELSE forecast_month END)  forecast_month_number
                          FROM idc_icm_cost_reporting
                          WHERE (((ds = (SELECT "max"(ds)
                                         FROM idc_icm_cost_reporting
                          )) AND (project_months >= ((
                              SELECT "date_trunc"('quarter', "date_parse"("max"(forecast_date), '%Y-%m-%d')) lock_month
                              FROM icm_cost_ctrl_cost_tblu
                              WHERE (ds = (SELECT "max"(ds)
                                           FROM icm_cost_ctrl_cost_tblu
                              ))
                          )))) AND (project_months < ((
                              SELECT "date_add"('quarter', 1, "date_trunc"('quarter', "date_parse"("max"(forecast_date), '%Y-%m-%d')))
                              FROM icm_cost_ctrl_cost_tblu
                              WHERE (ds = (SELECT "max"(ds)
                                           FROM icm_cost_ctrl_cost_tblu
                              ))
                          ))))
                      )
             )
        WHERE ((((((forecast_month_year_month = ((
            SELECT "date_format"("date_add"('month', 2, "date_add"('quarter', -1, "date_trunc"('quarter', "date_parse"("max"(forecast_date), '%Y-%m-%d')))), '%Y-%m')
            FROM icm_cost_ctrl_cost_tblu
            WHERE (ds = (SELECT "max"(ds)
                         FROM icm_cost_ctrl_cost_tblu
            ))
        ))) AND (type = 'Budget')) OR (type IN ('Actual', 'Forecast'))) AND (project_owner = 'New Builds')) AND (status IN ('Active', 'Planning'))) AND (locktype = 'Live'))
        GROUP BY site, locktype
        ORDER BY site ASC
    )
