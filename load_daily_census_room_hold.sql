CREATE OR REPLACE PROCEDURE reporting.load_daily_census_room_hold()
 LANGUAGE plpgsql
AS $$

/*
call  reporting.load_daily_census_room_hold()
;
select * 
from  reporting.daily_census_room_hold
where campusAbbreviation = 'AS'

*/
DECLARE
    v_census_date DATE := current_date - 1;
BEGIN
    -- Truncate target table first
    TRUNCATE TABLE reporting.daily_census_room_hold;

    drop table if exists temp_admissions;

    CREATE TEMP TABLE temp_admissions AS
    SELECT a.admissionname
        , a.admissionid
        , a.dischargetime
        , a.dischargedate
        , a.residentid
        , CAST(a.effectivedate AS DATE) AS leavestartdate
        , CAST(a.ineffectivedate AS DATE) AS leaveenddate
        , a.dischargelocationtypeid
        , aa.statuscode
        , aa.statusdescription
        , r.lastname 
    FROM dim.admission a
        inner join dim.admissionstatus aa on aa.admissionstatusid = a.admissionstatusid           
        inner join dim.resident r on r.residentid = a.residentid  
        inner join dim.campus c ON c.campusid = a.campusid
        inner join dim.levelofcare l ON l.levelofcareid = a.levelofcareid
    WHERE a.effectivedate::date = v_census_date
    AND l.levelofcareabbreviation = 'SNF'
    AND aa.statuscode in ('HUP', 'TUP', 'HP', 'TP', 'HN3') 
    AND r.deceaseddate IS NULL
    and a.admissionname not in (
        -- no discharges the same day
        SELECT distinct a.admissionname
        FROM dim.admission a
            JOIN dim.admissionstatus ac ON ac.admissionstatusid = a.admissionstatusid
            JOIN dim.campus c ON c.campusid = a.campusid
            JOIN dim.levelofcare l ON l.levelofcareid = a.levelofcareid
            -- JOIN dim.resident r ON r.residentid = a.residentid
        WHERE CAST(a.effectivedate AS DATE) = '2025-09-16'  --v_censusDate
            AND c.campusabbreviation = 'AS'--campusRecord.campusabbreviation
            AND l.levelofcareabbreviation = 'SNF'
            AND ac.statuscode not in ('D', 'DP')
    )       
    ;

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

    --  insert
    INSERT INTO reporting.daily_census_room_hold (
        fullname,
        admissionName,
        bed,
        leavestartdate,
        leaveenddate,
        leavedescription,
        payerabbreviation,
        date,
        campusabbreviation,
        census_date
    )

    
    SELECT DISTINCT
        r.fullname
        , ta.admissionname
        , COALESCE(v.roomname, '') || COALESCE(v.bedname, '') AS bed
        , ta.leavestartdate
        , ta.leaveenddate
        , ta.statusdescription
        , payer.payerabbreviation
        , ta.leavestartdate as date
        , v.campusabbreviation
        , v_census_date AS census_date
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

    DROP TABLE IF EXISTS temp_admissions;
    DROP TABLE IF EXISTS temp_bed_occupancy;

END;
$$
