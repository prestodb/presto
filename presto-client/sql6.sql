-- icm_pch_projects
    SELECT *
         , 'Lock' locktype
    FROM v_idc_eb_projects_fb projects
    WHERE (((((projects.ds = (SELECT "max"(forecast_date)
                              FROM icm_cost_ctrl_cost_tblu
                              WHERE (ds = (SELECT "max"(ds)
                                           FROM icm_cost_ctrl_cost_tblu
                              ))
    )) AND (projects.status IN ('Active', 'Planning'))) AND (projects.project_owner = 'New Builds')) AND (projects.site IS NOT NULL)) AND (projects.site <> ''))
    UNION ALL
    SELECT *
         , 'Live' locktype
    FROM v_idc_eb_projects_fb projects
    WHERE (((((projects.ds = (SELECT "max"(ds)
                              FROM v_idc_eb_projects_fb
    )) AND (projects.status IN ('Active', 'Planning'))) AND (projects.project_owner = 'New Builds')) AND (projects.site IS NOT NULL)) AND (projects.site <> ''))