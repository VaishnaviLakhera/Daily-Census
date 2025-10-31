CREATE OR REPLACE PROCEDURE reporting.load_daily_census_discharges()
 LANGUAGE plpgsql
AS $$

/*
call  reporting.load_daily_census_discharges()
;
select * 
from  reporting.daily_census_discharges
where campusAbbreviation = 'BV'

*/
DECLARE
    v_census_date DATE := current_date - 1;
BEGIN
    -- Truncate target table first
    TRUNCATE TABLE reporting.daily_census_discharges;

    -- Create a temp table for filtered admissions
    drop table if exists temp_admissions;

    CREATE TEMP TABLE temp_admissions AS
    SELECT a.admissionname
        , a.admissionid
        , a.dischargetime
        , a.dischargedate
        , a.residentid
        , a.dischargelocationtypeid
        , aa.statuscode
         , r.lastname 
    FROM dim.admission a
        inner join dim.admissionstatus aa on aa.admissionstatusid = a.admissionstatusid   
        inner join dim.admissionaction ac ON ac.admissionactionid = a.admissionactionid        
        inner join dim.resident r on r.residentid = a.residentid  
        inner join dim.campus c ON c.campusid = a.campusid
        inner join dim.levelofcare l ON l.levelofcareid = a.levelofcareid
    WHERE a.effectivedate::date = v_census_date
    AND l.levelofcareabbreviation = 'SNF'
    -- AND aa.statuscode = 'D'
    AND ac.actioncode = 'DD'
    AND r.deceaseddate IS NULL
    ;

    -- Create a temp table for bed occupancy matching discharge
    drop table if exists temp_bed_occupancy;
    
    CREATE TEMP TABLE temp_bed_occupancy AS
    SELECT 
        a.admissionname
        , bo.admissionid
        , bo.bedid
        , bo.date
        , bo.payerplanid
    FROM dim.bedoccupancy bo
        inner join dim.admission a on a.admissionid = bo.admissionid   
    WHERE bo.date >= v_census_date - 1
    ;

    -- Now insert into the target table using joins
    INSERT INTO reporting.daily_census_discharges (
        dischargetime,
        dischargedate,
        dischargetolocationname,
        fullname,
        residentid,
        bed,
        payerabbreviation,
        physician,
        buildingid,
        campusabbreviation,
        census_date
    )
    SELECT DISTINCT
        TO_CHAR(TO_TIMESTAMP('1900-01-01 ' || CAST(ta.dischargetime AS VARCHAR), 'YYYY-MM-DD HH24:MI:SS'), 'HH12:MI AM') AS dischargetime,
        ta.dischargedate,
        NULL::VARCHAR AS dischargetolocationname,
        r.fullname,
        r.residentid,
        COALESCE(v.roomname, '') || COALESCE(v.bedname, '') AS bed,
        payer.payerabbreviation,
        NULL::VARCHAR AS physician,
        v.buildingid,
        v.campusabbreviation ,
        v_census_date AS census_date
        --current_date - 1  census_date

--   select * 
    FROM temp_admissions ta
        INNER JOIN dim.resident r ON r.residentid = ta.residentid
        INNER JOIN temp_bed_occupancy tbo ON tbo.admissionname = ta.admissionname        
        inner join dim.vwbed v on v.bedid = tbo.bedid 
        INNER JOIN dim.payerplan pp ON pp.payerplanid = tbo.payerplanid
        INNER JOIN dim.payer payer ON payer.payerid = pp.payerid
    WHERE v.levelofcareabbreviation = 'SNF'
      AND r.deceaseddate IS NULL
    ;
      --AND c.campusabbreviation IN ('AMV'); -- You can change or expand campus filter if needed

    -- Drop temp tables (optional, Redshift drops temp tables at session end)
    DROP TABLE IF EXISTS temp_admissions;
    DROP TABLE IF EXISTS temp_bed_occupancy;

END;
$$
