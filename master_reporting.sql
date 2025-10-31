CREATE OR REPLACE PROCEDURE dim.master_reporting()
 LANGUAGE plpgsql
AS $$

BEGIN
/*
call dim.master_reporting();

*/
call dim.load_reportdistribution();

call reporting.load_daily_census_admissions();
call reporting.load_daily_census_census_by_unit();
call reporting.load_daily_census_deaths();
call reporting.load_daily_census_discharges();
call reporting.load_daily_census_payer_mix();
call reporting.load_daily_census_room_hold();
call reporting.load_daily_census_summary();




END;
$$
