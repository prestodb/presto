-- This query will fail in opt+ only because db2 has STARTSWITH function and not starts_with function.
select 1 from customer WHERE starts_with(C_COMMENT, 'us') LIMIT 1
