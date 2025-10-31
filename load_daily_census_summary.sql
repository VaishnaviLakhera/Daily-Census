CREATE OR REPLACE PROCEDURE reporting.load_daily_census_summary()
 LANGUAGE plpgsql
AS $$

/*
call reporting.load_daily_census_summary();

select * from reporting.daily_census_summary;

select * from reporting.daily_census_summary
where campusabbreviation = 'AS'
order by sortorder 

select current_date - 4;
;

*/


DECLARE
    -- Variables
    v_begingingOfDay			int := 0;
    v_inHouseCensus				int := 0;
    v_admissionCount			int := 0;
    v_readmissionCount			int := 0;
    v_dischargeCountInHouse		int := 0;
    v_dischargeCountPaid		int := 0;
    v_hospitalLOA				int := 0;
    v_endOfDayCensus			int := 0;
    v_total						int := 0;
    v_roomHoldYesterday			int := 0;
    v_newroomHold				int := 0;
    v_bedHold					int := 0;
    v_bedHoldSameDay			int := 0;
    v_deathCount				int := 0;
    v_rlHouse					int := 0;
    v_rlPaid					int := 0;
    v_tiHouse					int := 0;
    v_tiPaid					int := 0;
    v_hasTotalMismatch			varchar(50);
    v_occupancy					int := 0;

    campusRecord				RECORD;
    v_censusDate					date :=   current_date - 1;

BEGIN

    -- Truncate the target table
    TRUNCATE TABLE reporting.daily_census_summary;

    -- Loop through each campus with SNF level of care
    FOR campusRecord IN
        SELECT DISTINCT c.campusid
            , c.campusabbreviation
        FROM dim.campus c
        JOIN dim.levelofcare l ON l.campusid = c.campusid
        WHERE l.levelofcareabbreviation = 'SNF'
    LOOP

       select 0 into v_deathCount;


        -- Step 1: Create temp table with occupancy-related data
        DROP TABLE IF EXISTS temp_coreoccupancy;
         
        CREATE TEMP TABLE temp_coreoccupancy (
            date date ENCODE az64,
            occupied integer ENCODE az64,
            admissionid integer ENCODE az64,
            admissionname character varying(50) ENCODE lzo,
            effectivedate timestamp without time zone ENCODE az64,
            ineffectivedate timestamp without time zone ENCODE az64,
            dischargedate timestamp without time zone ENCODE az64,
            actioncode character varying(50) ENCODE lzo,
            admissionstatus character varying(50) ENCODE lzo,
            residentid integer ENCODE az64,
            deceaseddate date ENCODE az64,
            lastname character varying(255) ENCODE lzo,
            firstname character varying(255) ENCODE lzo,
            admissionnameto character varying(50) ENCODE lzo,
            admissionstatusto character varying(50) ENCODE lzo,
            campusabbreviation varchar(10)
        ) ;
        insert into temp_coreoccupancy (
            date,
            occupied,
            admissionid,
            admissionname,
            effectivedate,
            ineffectivedate,
            dischargedate,
            actioncode,
            admissionstatus,
            residentid,
            deceaseddate,
            lastname,
            firstname,
            admissionnameto,
            admissionstatusto,
            campusabbreviation
        )
        SELECT DISTINCT
            bo.date,
            bo.occupied,
            a.admissionid,
            a.admissionname,
            a.effectivedate,
            a.ineffectivedate,
            a.dischargedate,
            aa.actioncode,
            ac.statuscode AS admissionstatus,
            r.residentid,
            r.deceaseddate,
            r.lastname,
            r.firstname,
            ah.admissionname AS admissionnameto,
            ach.statuscode  AS admissionstatusto,
            campusRecord.campusabbreviation
        FROM dim.bedoccupancy bo
            JOIN dim.admission a ON a.admissionid = bo.admissionid
            JOIN dim.bed b ON b.bedid = bo.bedid
            JOIN dim.room ro ON ro.roomid = b.roomid
            JOIN dim.unit u ON u.unitid = ro.unitid
            JOIN dim.building bld ON bld.buildingid = u.buildingid
            JOIN dim.levelofcare loc ON loc.levelofcareid = bld.levelofcareid
            JOIN dim.campus c ON c.campusid = loc.campusid
            JOIN dim.admissionaction aa ON aa.admissionactionid = a.admissionactionid
            JOIN dim.admissionstatus ac ON ac.admissionstatusid = a.admissionstatusid
            JOIN dim.resident r ON r.residentid = a.residentid
            LEFT JOIN dim.admission ah ON ah.admissionname = a.admissionname
                AND CAST(ah.effectivedate AS DATE) = v_censusDate + 1
                AND ah.effectivedate IS NOT NULL
                AND ah.ineffectivedate IS NOT NULL
                AND ah.effectivedate::date <> ah.ineffectivedate::date
            LEFT JOIN dim.admissionstatus ach ON ach.admissionstatusid = ah.admissionstatusid
                AND ach.statuscode IN ('HUP', 'HN3', 'TUP')
        WHERE loc.levelofcareabbreviation = 'SNF'
        AND c.campusabbreviation = campusRecord.campusabbreviation
        AND CAST(bo.date AS DATE) >= (v_censusDate - 5);




        -- Beginning of Day & Room Hold Yesterday
        SELECT
            SUM(occupied),
            SUM(CASE WHEN admissionstatus IN ('HP', 'TP', 'MTL') THEN 1 ELSE 0 END)            
        INTO v_begingingOfDay, v_roomHoldYesterday
        FROM temp_coreoccupancy
        WHERE CAST(date AS DATE) = v_censusDate - 1
        AND campusabbreviation = campusRecord.campusabbreviation
        ;
     

        SELECT
            COALESCE(SUM(CASE WHEN admissionstatus IN ('HP', 'TP', 'MTL') THEN 1 ELSE 0 END), 0)
        INTO v_newroomHold
        FROM temp_coreoccupancy
        WHERE CAST(effectivedate AS DATE) = v_censusDate
            AND CAST(date AS DATE) = v_censusDate;

        -- Admissions & Readmissions
        SELECT
            SUM(CASE WHEN actioncode = 'AA' AND CAST(effectivedate AS DATE) = v_censusDate THEN 1 ELSE 0 END),
            SUM(CASE WHEN actioncode = 'RA' AND CAST(effectivedate AS DATE) = v_censusDate THEN 1 ELSE 0 END)
        INTO v_admissionCount, v_readmissionCount
        FROM temp_coreoccupancy
        WHERE CAST(date AS DATE) = v_censusDate;

        -- Return/Transfer In
        SELECT
            SUM(CASE WHEN a.actioncode = 'RL' AND a.admissionstatus = 'A'
                        AND CAST(a.effectivedate AS DATE) = v_censusDate
                        AND b.admissionstatus IN ('HUP', 'HN3', 'TUP', 'HP', 'TP') THEN 1 ELSE 0 END),
            SUM(CASE WHEN a.actioncode = 'RL' AND a.admissionstatus = 'A'
                        AND CAST(a.effectivedate AS DATE) = v_censusDate
                        AND b.admissionstatus IN ('HP', 'TP') THEN 1 ELSE 0 END)
        INTO v_rlHouse, v_rlPaid
        FROM temp_coreoccupancy a
        JOIN temp_coreoccupancy b ON b.admissionname = a.admissionname
            AND b.date = a.date - INTERVAL '1 day'
        WHERE CAST(a.date AS DATE) = v_censusDate;

        -- Transfer In
        SELECT
            COALESCE(SUM(CASE WHEN c.admissionstatus = 'A' AND ac.statuscode IN ('HUP', 'HN3', 'TUP', 'HP', 'TP') THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN c.admissionstatus = 'A' AND ac.statuscode IN ('HUP', 'TUP', 'HN3') THEN 1 ELSE 0 END), 0)
        INTO v_tiHouse, v_tiPaid
        FROM temp_coreoccupancy c
            JOIN dim.admission a ON a.admissionname = c.admissionname
                AND CAST(a.effectivedate AS DATE) = c.date
            JOIN dim.admissionaction aa ON aa.admissionactionid = a.admissionactionid
                AND aa.actioncode = 'TI'
            JOIN dim.admission a2 ON a2.admissionname = c.admissionname
                AND CAST(a2.ineffectivedate AS DATE) = c.date
            JOIN dim.admissionstatus ac ON ac.admissionstatusid = a2.admissionstatusid
                AND ac.statuscode IN ('HUP', 'HN3', 'TUP', 'HP', 'TP')
        WHERE c.date = v_censusDate;

        -- Hospital LOA
        -- SELECT COUNT(*)
        -- INTO v_hospitalLOA
        -- FROM temp_coreoccupancy
        -- WHERE CAST(date AS DATE) = v_censusDate 
        --     AND admissionstatusto IN ('HUP', 'HN3', 'TUP');

        SELECT COUNT(distinct  a.admissionname)
        INTO v_hospitalLOA        
        -- select r.fullname, a.admissionname, ac.statuscode  
        FROM dim.admission a
            JOIN dim.admissionstatus ac ON ac.admissionstatusid = a.admissionstatusid
            JOIN dim.campus c ON c.campusid = a.campusid
            JOIN dim.levelofcare l ON l.levelofcareid = a.levelofcareid
            -- JOIN dim.resident r ON r.residentid = a.residentid
        WHERE CAST(a.effectivedate AS DATE) = v_censusDate
        AND c.campusabbreviation = campusRecord.campusabbreviation
        AND l.levelofcareabbreviation = 'SNF'
        AND ac.statuscode in ('HUP', 'HN3', 'TUP')
        and a.admissionname not in (
            -- no HN3 yesterday
            SELECT distinct a.admissionname
            FROM dim.admission a
                JOIN dim.admissionstatus ac ON ac.admissionstatusid = a.admissionstatusid
                JOIN dim.campus c ON c.campusid = a.campusid
                JOIN dim.levelofcare l ON l.levelofcareid = a.levelofcareid
                -- JOIN dim.resident r ON r.residentid = a.residentid
            WHERE CAST(a.effectivedate AS DATE) = v_censusDate - 1
                AND c.campusabbreviation = campusRecord.campusabbreviation
                AND l.levelofcareabbreviation = 'SNF'
                AND ac.statuscode in ('HN3')
        )
        and a.admissionname not in (
            -- no discharges the same day
            SELECT distinct a.admissionname
            FROM dim.admission a
                JOIN dim.admissionstatus ac ON ac.admissionstatusid = a.admissionstatusid
                JOIN dim.campus c ON c.campusid = a.campusid
                JOIN dim.levelofcare l ON l.levelofcareid = a.levelofcareid
                -- JOIN dim.resident r ON r.residentid = a.residentid
            WHERE CAST(a.effectivedate AS DATE) = v_censusDate
            AND c.campusabbreviation = campusRecord.campusabbreviation
            AND l.levelofcareabbreviation = 'SNF'
            AND ac.statuscode in ('D', 'DP')
            
        ) 
            ;



        -- Discharges (both InHouse and Paid)
/*
        SELECT COUNT(ac.actioncode), COUNT(ac.actioncode)
        INTO v_dischargeCountPaid, v_dischargeCountInHouse
        FROM dim.admission a
            JOIN dim.admissionaction ac ON ac.admissionactionid = a.admissionactionid
            JOIN dim.campus c ON c.campusid = a.campusid
            JOIN dim.levelofcare l ON l.levelofcareid = a.levelofcareid
            JOIN dim.resident r ON r.residentid = a.residentid
        WHERE CAST(a.effectivedate AS DATE) = v_censusDate
        AND c.campusabbreviation = campusRecord.campusabbreviation
        AND l.levelofcareabbreviation = 'SNF'
        AND ac.actioncode = 'DD'
        AND r.deceaseddate IS NULL;
*/


        SELECT COUNT(ac.actioncode), COUNT(ac.actioncode)
        INTO v_dischargeCountPaid, v_dischargeCountInHouse
        -- select * 
        FROM dim.admission a
            JOIN dim.admissionaction ac ON ac.admissionactionid = a.admissionactionid
            JOIN dim.campus c ON c.campusid = a.campusid
            JOIN dim.levelofcare l ON l.levelofcareid = a.levelofcareid
            JOIN dim.resident r ON r.residentid = a.residentid
        WHERE CAST(a.effectivedate AS DATE) = v_censusDate
        AND c.campusabbreviation = campusRecord.campusabbreviation
        AND l.levelofcareabbreviation = 'SNF'
        AND ac.actioncode = 'DD'
        AND r.deceaseddate IS NULL
        and a.admissionName not in (
            select a.admissionName 
            from dim.admission a
                JOIN dim.admissionstatus aa ON aa.admissionstatusid = a.admissionstatusid
            where a.effectiveDate::date >= dateAdd(d, -1, v_censusDate)
            and (a.ineffectiveDate::date <= v_censusDate
            or a.ineffectiveDate::date is null)
            AND aa.statuscode = 'HN3'
        )
        ;


        -- Deaths
        -- SELECT COUNT(DISTINCT admissionid)
        -- INTO v_deathCount
        -- FROM temp_coreoccupancy
        -- WHERE CAST(dischargedate AS DATE) = v_censusDate
        --     AND deceaseddate IS NOT NULL
        --     AND CAST(date AS DATE) = v_censusDate - 1;

        -- Deaths
        -- SELECT COUNT(DISTINCT a.admissionname)
        -- INTO v_deathCount
        -- -- select a.admissionName
        -- -- select max(bo.date)
        -- FROM dim.admission a
        --     JOIN dim.admissionstatus ac ON ac.admissionstatusid = a.admissionstatusid
        --     JOIN dim.campus c ON c.campusid = a.campusid
        --     JOIN dim.levelofcare l ON l.levelofcareid = a.levelofcareid
        --     JOIN dim.resident r ON r.residentid = a.residentid
        --     join dim.admission a2 on a2.admissionName = a.admissionName 
        --     join dim.bedoccupancy bo on bo.admissionid = a2.admissionid
        -- WHERE CAST(r.deceaseddate AS DATE) = v_censusDate
        --     AND c.campusabbreviation = campusRecord.campusabbreviation
        --     AND l.levelofcareabbreviation = 'SNF'
        --     and bo.date = v_censusDate -1  
        --     ;


        SELECT COUNT(DISTINCT bo.admissionname)
        INTO v_deathCount
        -- select a.admissionName
        -- select max(bo.date)
        -- select bo.*
        FROM temp_coreoccupancy bo
        WHERE bo.campusabbreviation = campusRecord.campusabbreviation
        and CAST( bo.deceaseddate  AS DATE) = v_censusDate
        and CAST(bo.date AS DATE) >= v_censusDate - 1
        ;





        -- Bed Hold
        SELECT COUNT(DISTINCT admissionname)
        INTO v_bedHold
        FROM temp_coreoccupancy
        WHERE CAST(date AS DATE) = v_censusDate
            AND CAST(date AS DATE) = CAST(effectivedate AS DATE)
            AND admissionstatus IN ('HP', 'TP', 'MTL');

        -- Final Calculations
        v_inHouseCensus := v_begingingOfDay - v_roomHoldYesterday;
        --v_roomHoldYesterday := v_roomHoldYesterday + v_newroomHold;
        v_endOfDayCensus := v_inHouseCensus + v_admissionCount + v_readmissionCount - v_dischargeCountInHouse - v_deathCount - v_hospitalLOA - v_bedHold + v_rlHouse + v_tiHouse;
        v_occupancy := v_inHouseCensus + v_roomHoldYesterday + v_admissionCount + v_readmissionCount - v_dischargeCountPaid - v_deathCount - v_hospitalLOA + v_tiPaid;

        IF v_endOfDayCensus != v_total OR v_occupancy != v_total THEN
            v_hasTotalMismatch := 'true';
        ELSE
            v_hasTotalMismatch := 'false';
        END IF;

        RAISE NOTICE 'v_inHouseCensus: %', v_inHouseCensus;

        --select count(*) from   temp_coreoccupancy;

        -- Insert into final table
        INSERT INTO reporting.daily_census_summary (
            census_date
            , campusabbreviation
            , sortorder
            , displaytext1
            , displaytext2
            , displayvalue
            , total
            , hasTotalMismatch
            , PaidBedHold
            , SubTotal
            , EndRow
        )
        SELECT 
            v_censusDate
            , campusRecord.campusabbreviation
            , sortorder
            , displaytext1
            , displaytext2
            , displayvalue
            , total
            , hasTotalMismatch
            , PaidBedHold
            , SubTotal
            , EndRow
        FROM (
                SELECT 1 sortorder, 'Beginning of Day' as displaytext1, 'In House' as displaytext2, v_inHouseCensus as displayvalue, v_total as total, v_hasTotalMismatch as  hasTotalMismatch, v_bedHold as PaidBedHold, '' as SubTotal, v_inHouseCensus as EndRow
                UNION ALL
                SELECT 2, '',                    'Paid Bed Hold', NULL, v_total, v_hasTotalMismatch, v_bedHold, '', v_roomHoldYesterday
                UNION ALL
                SELECT 3, '',                    '', v_inHouseCensus, v_total, v_hasTotalMismatch, v_bedHold, '1', v_inHouseCensus + v_roomHoldYesterday

                -- Add
                UNION ALL
                SELECT 4, 'Add:',                'Admissions', v_admissionCount, v_total, v_hasTotalMismatch, v_bedHold, '', v_admissionCount
                UNION ALL
                SELECT 5, NULL,                 'Redmissions', v_readmissionCount, v_total, v_hasTotalMismatch, v_bedHold, '', v_readmissionCount
                UNION ALL
                SELECT 6, NULL,     'Return from Hospital/LOA', v_rlHouse + v_tiHouse, v_total, v_hasTotalMismatch, v_bedHold, '', v_tiPaid
                UNION ALL
                SELECT 7, '',                    '', v_admissionCount + v_readmissionCount + v_rlHouse + v_tiHouse, v_total, v_hasTotalMismatch, v_bedHold, '1', v_admissionCount + v_readmissionCount

                -- Deduct
                UNION ALL
                SELECT 8, 'Deduct:',        'Discharge', v_dischargeCountInHouse, v_total, v_hasTotalMismatch, v_bedHold, '', v_dischargeCountPaid
                UNION ALL
                SELECT 9, NULL,             'Death', v_deathCount, v_total, v_hasTotalMismatch, v_bedHold, '', v_deathCount
                UNION ALL
                SELECT 10, NULL,            'LOA', v_hospitalLOA + v_bedHold, v_total, v_hasTotalMismatch, v_bedHold, '', v_hospitalLOA
                UNION ALL
                SELECT 11, '',              '', v_dischargeCountInHouse + v_deathCount + v_hospitalLOA + v_bedHold, v_total, v_hasTotalMismatch, v_bedHold, '1', v_dischargeCountPaid + v_deathCount + v_hospitalLOA

                -- End of Day
                UNION ALL
                SELECT 12, 'End of Day', 'Total', v_endOfDayCensus, v_total, v_hasTotalMismatch, v_bedHold, '1', v_occupancy

            ) AS results ;
            -- (
            --     sortorder
            --     , displaytext1
            --     , displaytext2
            --     , displayvalue
            --     , total
            --     , hasTotalMismatch
            --     , PaidBedHold
            --     , SubTotal
            --     , EndRow
            -- )
            -- -- Add constant values for date and campus
            -- , v_census_date, v_campus_abbr;

            -- Clean up temp table
            DROP TABLE IF EXISTS temp_core_occupancy;

            
        --) AS final;

    END LOOP;

END;
$$
