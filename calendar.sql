CREATE
OR REPLACE TABLE investment.calendar (
    `date` DATE NOT NULL,
    is_business_day_br BOOLEAN NOT NULL,
    week_day INT64 NOT NULL,
    lag_business_day_br DATE NOT NULL,
    lead_business_day_br DATE NOT NULL,
    first_business_day_br DATE NOT NULL,
    last_business_day_br DATE NOT NULL
);

INSERT INTO
    investment.calendar (
        date,
        is_business_day_br,
        week_day,
        lag_business_day_br,
        lead_business_day_br,
        first_business_day_br,
        last_business_day_br
    ) WITH holidays AS (
        SELECT
            DISTINCT holiday_date
        FROM
            hub.holidays
    ),
    calendar AS (
        SELECT
            calendar_date AS `date`,
            CASE
                WHEN EXTRACT(
                    DAYOFWEEK
                    FROM
                        calendar_date
                ) IN (1, 7) THEN FALSE -- Sunday or Saturday
                WHEN holiday_date IS NOT NULL THEN FALSE -- Holiday
                ELSE TRUE -- Business day
            END AS is_business_day_br,
            EXTRACT(
                DAYOFWEEK
                FROM
                    calendar_date
            ) AS week_day
        FROM
            UNNEST(GENERATE_DATE_ARRAY('2000-12-29', '2099-12-31')) AS calendar_date
            LEFT JOIN holidays ON calendar_date = holidays.holiday_date
    ),
    cte_lag_lead_date_1 AS (
        SELECT
            `date`,
            is_business_day_br,
            week_day,
            IF(
                is_business_day_br = TRUE,
                `date`,
                LAG(`date`) OVER (
                    ORDER BY
                        `date`
                )
            ) AS lag_date_1,
            IF(
                is_business_day_br = TRUE,
                `date`,
                LEAD(`date`) OVER (
                    ORDER BY
                        `date`
                )
            ) AS lead_date_1
        FROM
            calendar
    ),
    cte_lag_lead_date_2 AS (
        SELECT
            l.`date`,
            l.is_business_day_br,
            l.week_day,
            IF(
                c.is_business_day_br = TRUE,
                l.lag_date_1,
                LAG(l.lag_date_1) OVER (
                    ORDER BY
                        l.lag_date_1
                )
            ) AS lag_date_2,
            IF(
                cc.is_business_day_br = TRUE,
                l.lead_date_1,
                LEAD(l.lead_date_1) OVER (
                    ORDER BY
                        l.lead_date_1
                )
            ) AS lead_date_2
        FROM
            cte_lag_lead_date_1 AS l
            LEFT JOIN calendar AS c ON l.lag_date_1 = c.`date`
            LEFT JOIN calendar AS cc ON l.lead_date_1 = cc.`date`
    ),
    cte_lag_lead_date_3 AS (
        SELECT
            l.`date`,
            l.is_business_day_br,
            l.week_day,
            IF(
                c.is_business_day_br = TRUE,
                l.lag_date_2,
                LAG(l.lag_date_2) OVER (
                    ORDER BY
                        l.lag_date_2
                )
            ) AS lag_date_3,
            IF(
                cc.is_business_day_br = TRUE,
                l.lead_date_2,
                LEAD(l.lead_date_2) OVER (
                    ORDER BY
                        l.lead_date_2
                )
            ) AS lead_date_3
        FROM
            cte_lag_lead_date_2 AS l
            LEFT JOIN calendar AS c ON l.lag_date_2 = c.`date`
            LEFT JOIN calendar AS cc ON l.lead_date_2 = cc.`date`
    ),
    cte_lag_lead_date_4 AS (
        SELECT
            l.`date`,
            l.is_business_day_br,
            l.week_day,
            IF(
                c.is_business_day_br = TRUE,
                l.lag_date_3,
                LAG(l.lag_date_3) OVER (
                    ORDER BY
                        l.lag_date_3
                )
            ) AS lag_date_4,
            IF(
                cc.is_business_day_br = TRUE,
                l.lead_date_3,
                LEAD(l.lead_date_3) OVER (
                    ORDER BY
                        l.lead_date_3
                )
            ) AS lead_date_4
        FROM
            cte_lag_lead_date_3 AS l
            LEFT JOIN calendar AS c ON l.lag_date_3 = c.`date`
            LEFT JOIN calendar AS cc ON l.lead_date_3 = cc.`date`
    )
SELECT
    `date`,
    is_business_day_br,
    week_day,
    lag_date_4 AS lag_business_day_br,
    lead_date_4 AS lead_business_day_br,
    MIN(lead_date_4) OVER (PARTITION BY DATE_TRUNC(`date`, MONTH)) AS first_business_day_br,
    MAX(lag_date_4) OVER (PARTITION BY DATE_TRUNC(`date`, MONTH)) AS last_business_day_br
FROM
    cte_lag_lead_date_4