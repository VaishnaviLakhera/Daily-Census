CREATE OR REPLACE PROCEDURE reporting.load_daily_census_deaths()
 LANGUAGE plpgsql
AS $$

/*

call  reporting.load_daily_census_deaths()

select * 
from  reporting.daily_census_deaths
where campusAbbreviation = 'AMV'

*/


DECLARE
    v_census_date DATE := current_date - 1;
BEGIN
    -- Truncate target table
    TRUNCATE TABLE reporting.daily_census_deaths;

    -- Create temp table for processing
    CREATE TEMP TABLE tmp_daily_census_deaths AS
    SELECT DISTINCT
         r.fullname
        ,NULL::INTEGER AS residentid
        ,COALESCE(ro.roomname, '') || COALESCE(b.bedname, '') AS bed
        ,payer.payerabbreviation
        ,NULL::VARCHAR AS physician
        ,r.deceaseddate
        ,c.campusabbreviation
        ,v_census_date AS census_date
    FROM dim.bedoccupancy bo
        INNER JOIN dim.admission a ON a.admissionid = bo.admissionid
        INNER JOIN dim.resident r ON r.residentid = a.residentid
        INNER JOIN dim.bed b ON b.bedid = bo.bedid
        INNER JOIN dim.room ro ON ro.roomid = b.roomid
        INNER JOIN dim.unit u ON u.unitid = ro.unitid
        INNER JOIN dim.building bld ON bld.buildingid = u.buildingid
        INNER JOIN dim.levelofcare loc ON loc.levelofcareid = bld.levelofcareid
        INNER JOIN dim.campus c ON c.campusid = loc.campusid
        INNER JOIN dim.payerplan pp ON pp.payerplanid = bo.payerplanid
        INNER JOIN dim.payer ON payer.payerid = pp.payerid
    WHERE
    --CAST(a.dischargedate AS DATE) = v_census_date
    CAST( r.deceaseddate  AS DATE) = v_census_date
    AND CAST(bo.date AS DATE) = v_census_date - 1
    AND loc.levelofcareabbreviation = 'SNF'
    ;

    -- Insert into final table
    INSERT INTO reporting.daily_census_deaths (
         fullname
        ,residentid
        ,bed
        ,payerabbreviation
        ,physician
        ,deceaseddate
        ,campusabbreviation
        ,census_date
    )
    SELECT
         fullname
        ,residentid
        ,bed
        ,payerabbreviation
        ,physician
        ,deceaseddate
        ,campusabbreviation
        ,census_date
    FROM tmp_daily_census_deaths;

    -- Clean up
    DROP TABLE IF EXISTS tmp_daily_census_deaths;

END;
$$
