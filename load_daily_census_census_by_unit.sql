CREATE OR REPLACE PROCEDURE reporting.load_daily_census_census_by_unit()
 LANGUAGE plpgsql
AS $$

/*
call  reporting.load_daily_census_census_by_unit()

select * 
from  reporting.daily_census_census_by_unit
where campusAbbreviation = 'AMV'

*/


DECLARE
    v_census_date DATE := CURRENT_DATE - INTERVAL '1 day';
BEGIN
    -- Truncate the destination table
    TRUNCATE TABLE reporting.daily_census_census_by_unit;

    -- Temporary table to store intermediate results
    CREATE TEMP TABLE tmp_census_by_unit AS
    SELECT 
        REPLACE(u.unitname, 'MC ', '') AS unitname,
        bo.date AS census_date,
        SUM(bo.available) AS available,
        SUM(bo.occupied) - SUM(CASE 
                                  WHEN ac.statusdescription IN ('Hospital Paid Leave','Therapeutic Paid Leave') 
                                  THEN 1 ELSE 0 
                              END) AS occupied,
        SUM(bo.occupied) AS total,
        SUM(bo.available) - SUM(bo.occupied) AS vacancy,
        SUM(CASE 
                WHEN ac.statusdescription IN ('Hospital Paid Leave','Therapeutic Paid Leave') 
                THEN 1 ELSE 0 
            END) AS bedhold,
        c.campusabbreviation
    FROM dim.bedoccupancy bo
        INNER JOIN dim.bed b ON b.bedid = bo.bedid
        INNER JOIN dim.room ro ON ro.roomid = b.roomid
        INNER JOIN dim.unit u ON u.unitid = ro.unitid
        INNER JOIN dim.building bld ON bld.buildingid = u.buildingid
        INNER JOIN dim.levelofcare loc ON loc.levelofcareid = bld.levelofcareid
        INNER JOIN dim.campus c ON c.campusid = loc.campusid
        LEFT JOIN dim.admission a ON a.admissionid = bo.admissionid
        LEFT JOIN dim.admissionstatus ac ON ac.admissionstatusid = a.admissionstatusid
    WHERE CAST(bo.date AS DATE) = v_census_date
      AND loc.levelofcareabbreviation = 'SNF'
    GROUP BY REPLACE(u.unitname, 'MC ', ''), bo.date, c.campusabbreviation;

    -- Insert only relevant rows into the permanent table
    INSERT INTO reporting.daily_census_census_by_unit (
        unitname
        , census_date
        , available
        , occupied
        , total
        , vacancy
        , bedhold
        , campusabbreviation
    )
    SELECT 
        unitname
        , census_date
        , available
        , occupied
        , total
        , vacancy
        , bedhold
        , campusabbreviation
    FROM tmp_census_by_unit
    WHERE (available + occupied) > 0;

    -- Drop the temp table
    DROP TABLE tmp_census_by_unit;

END;
$$
