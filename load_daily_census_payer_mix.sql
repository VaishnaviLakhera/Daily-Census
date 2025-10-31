CREATE OR REPLACE PROCEDURE reporting.load_daily_census_payer_mix()
 LANGUAGE plpgsql
AS $$

/*
call reporting.load_daily_census_payer_mix()

select * 
from  reporting.daily_census_payer_mix 
where campusabbreviation = 'AS'
order by 3

*/

DECLARE
    censusDate DATE := current_date - 1;
    rec RECORD;
    total DECIMAL(18,4);
BEGIN
    -- Truncate the target table
    TRUNCATE TABLE reporting.daily_census_payer_mix;

    -- Loop over each campus
    FOR rec IN SELECT DISTINCT campusabbreviation FROM dim.campus
    LOOP
        -- Temp table for payer
        CREATE TEMP TABLE tmp_payer AS
        SELECT payerid, payername
        FROM dim.payer;

        -- Temp table for occupancy
        CREATE TEMP TABLE tmp_occupied AS
        SELECT 
            COALESCE(pp.payerid, 0) AS payerid,
            CASE 
                WHEN upper(p.payername) = 'MEDICAREA/MC HMO' THEN 'Medicare A' 
                WHEN upper(p.payername) = 'PRIVATE PAY' THEN 'Private' 
                WHEN upper(p.payername) = 'MANAGED MEDICARE' THEN 'Medicare Advantage'
                ELSE p.payername 
            END AS payername,
            SUM(bo.occupied) AS occupied
        FROM dim.bedoccupancy bo 
            LEFT JOIN dim.payerplan pp ON pp.payerplanid = bo.payerplanid
            LEFT JOIN dim.payer p ON p.payerid = pp.payerid
            INNER JOIN dim.bed b ON b.bedId = bo.bedid
            INNER JOIN dim.room ro ON ro.roomid = b.roomid
            INNER JOIN dim.unit u ON u.unitid = ro.unitid
            INNER JOIN dim.building bld ON bld.buildingid = u.buildingid
            INNER JOIN dim.levelofcare loc ON loc.levelofcareid = bld.levelofcareid
            INNER JOIN dim.campus c ON c.campusid = loc.campusid
        WHERE CAST(bo.date AS DATE) = censusDate
        AND loc.levelofcareabbreviation = 'SNF'
        AND c.campusabbreviation = rec.campusabbreviation
        GROUP BY COALESCE(pp.payerid, 0)
            , p.payername
            ;


        -- Temp table for budget
        CREATE TEMP TABLE tmp_budget AS
        SELECT 
            --bp.payerid,
            CASE 
                WHEN upper(bp.payername) = 'MEDICAREA/MC HMO' THEN 'Medicare A' 
                WHEN upper(bp.payername) = 'PRIVATE PAY' THEN 'Private' 
                WHEN upper(bp.payername) = 'MANAGED MEDICARE' THEN 'Medicare Advantage' 
                ELSE bp.payername 
            END AS payername,
            SUM(bp.budgetplandays) AS budget
            , pp.payerid 
        FROM dim.budgetplandays bp
            INNER JOIN dim.building bld ON bld.buildingid = bp.buildingid
            INNER JOIN dim.levelofcare loc ON loc.levelofcareid = bld.levelofcareid
            INNER JOIN dim.campus c ON c.campusid = loc.campusid
            inner join dim.payerPlan pp on pp.payerplanid = bp.payerplanid 
        -- WHERE censusDate BETWEEN CAST(bp.startdate AS DATE) AND CAST(bp.enddate AS DATE)
        where CAST(bp.startdate AS DATE) <= censusDate  --'2025-09-15'
        and CAST(bp.enddate AS DATE) >= censusDate -- '2025-09-15'
          AND loc.levelofcareabbreviation = 'SNF'
          AND c.campusabbreviation = rec.campusabbreviation
        GROUP BY bp.payerid
            , bp.payername
            , pp.payerid 
        ;



        -- Temp table for unioned payers
        CREATE TEMP TABLE tmp_all_payers AS
        SELECT payerid, payername FROM tmp_payer
        UNION
        SELECT 0 AS payerid, 'unknown' AS payername;

        -- Temp table for result
        CREATE TEMP TABLE tmp_result AS
        SELECT 
            ap.payername,
            COALESCE(b.budget, 0)::DECIMAL(18,2) AS budget,
            COALESCE(o.occupied, 0)::DECIMAL(18,2) AS census,
            (COALESCE(o.occupied, 0) - COALESCE(b.budget, 0))::DECIMAL(18,2) AS variance
        FROM tmp_all_payers ap
        LEFT JOIN tmp_budget b ON b.payername = ap.payername
        LEFT JOIN tmp_occupied o ON o.payername = ap.payername
        WHERE NOT (COALESCE(b.budget, 0) = 0 AND COALESCE(o.occupied, 0) = 0);

        -- Calculate total
        SELECT SUM(census) INTO total FROM tmp_result;

        -- Insert into final table
        INSERT INTO reporting.daily_census_payer_mix (
            payername,
            budget,
            census,
            variance,
            censusmix,
            campusabbreviation,
            census_date
        )
        SELECT 
            payername,
            budget,
            census,
            variance,
            CASE 
                WHEN total > 0 THEN TO_CHAR((census / total) * 100, 'FM99999990.00') || '%'
                ELSE '0%'
            END AS censusmix,
            rec.campusabbreviation,
            censusDate
        FROM tmp_result;

        -- Drop temp tables
        DROP TABLE IF EXISTS tmp_payer;
        DROP TABLE IF EXISTS tmp_occupied;
        DROP TABLE IF EXISTS tmp_budget;
        DROP TABLE IF EXISTS tmp_all_payers;
        DROP TABLE IF EXISTS tmp_result;
    END LOOP;
END;
$$
