SELECT
    '33ce2ffa-61f0-11ee-8c99-0242ac120002' AS id,
    'cdi' AS `name`,
    'indice financeiro' AS type,
    CAST(NULL AS INT) AS fund_id,
    CAST(NULL AS STRING) AS first_set,
    CAST(NULL AS STRING) AS second_set,
    CAST(NULL AS STRING) AS third_set,
    CAST(NULL AS STRING) AS fourth_set,
    CAST(dt_reference AS DATE) AS reference_dt,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    MAX(CAST(cdi AS FLOAT64)) AS `value`
FROM
    investment.financial_indexes
WHERE
    cdi IS NOT NULL
GROUP BY
    reference_dt
