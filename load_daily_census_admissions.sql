CREATE OR REPLACE PROCEDURE reporting.load_daily_census_admissions()
 LANGUAGE plpgsql
AS $$

/*

call  reporting.load_daily_census_admissions()

select * 
from  reporting.daily_census_admissions
where campusAbbreviation = 'AMV'

*/

DECLARE
    v_census_date DATE := CURRENT_DATE - INTERVAL '1 day';
    rec_campus RECORD;
BEGIN
    -- Truncate target table
    TRUNCATE TABLE reporting.daily_census_admissions;

    -- Loop over campuses
    FOR rec_campus IN 
        SELECT DISTINCT campusabbreviation FROM dim.campus
    LOOP
        -- Create temp table for admissions for this campus
        CREATE TEMP TABLE temp_admissions AS
        SELECT DISTINCT
            TO_CHAR(CAST('1900-01-01 ' || a.admissiontime AS TIMESTAMP), 'HH12:MI AM') AS admissiontime,
            r.fullname,
            NULL::VARCHAR AS residentid,
            NULL::VARCHAR AS admissionfromlocationname,
            COALESCE(ro.roomname, '') || COALESCE(b.bedname, '') AS bed,
            COALESCE(payer.payerabbreviation, 'unknown') AS payerabbreviation,
            NULL::VARCHAR AS physician,
            bld.buildingid,
            c.campusabbreviation
        FROM dim.admission a
            INNER JOIN dim.resident r ON r.residentid = a.residentid
            INNER JOIN dim.bedoccupancy bo ON bo.admissionid = a.admissionid
                AND bo.date = a.admissiondate::date
            INNER JOIN dim.bed b ON b.bedid = bo.bedid
            INNER JOIN dim.room ro ON ro.roomid = b.roomid
            INNER JOIN dim.unit u ON u.unitid = ro.unitid
            INNER JOIN dim.building bld ON bld.buildingid = u.buildingid
            INNER JOIN dim.levelofcare loc ON loc.levelofcareid = bld.levelofcareid
            INNER JOIN dim.campus c ON c.campusid = loc.campusid
            LEFT JOIN dim.payerplan pp ON pp.payerplanid = bo.payerplanid
            LEFT JOIN dim.payer payer ON payer.payerid = pp.payerid
        WHERE a.admissiondate::date = v_census_date
        AND loc.levelofcareabbreviation = 'SNF'
        AND c.campusabbreviation = rec_campus.campusabbreviation;

        -- Insert into permanent table with census_date
        INSERT INTO reporting.daily_census_admissions (
            admissiontime,
            fullname,
            residentid,
            admissionfromlocationname,
            bed,
            payerabbreviation,
            physician,
            buildingid,
            campusabbreviation,
            census_date
        )
        SELECT
            admissiontime,
            fullname,
            residentid,
            admissionfromlocationname,
            bed,
            payerabbreviation,
            physician,
            buildingid,
            campusabbreviation,
            v_census_date
        FROM temp_admissions;

        DROP TABLE IF EXISTS temp_admissions;
    END LOOP;
END;
$$
