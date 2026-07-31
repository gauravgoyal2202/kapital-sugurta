{{ config(materialized='table') }}

/*
  Curated: Priority Areas raw detail — exact output of original query.
  General/OSAGO-new payments use curated_partner_payment_base (inline fifty / kurs).
  Consumed by mart_commercial_development_priority_areas_base.
*/

WITH claims_by_anketa AS (
    SELECT
        vipl.anketa_id,
        SUM(COALESCE(vipl.decision_summa, 0)) AS claim_value
    FROM {{ source('raw', 'ins_loss_oracle') }} vipl
    WHERE vipl.anketa_id IS NOT NULL
    GROUP BY vipl.anketa_id
),

claim_dates_by_anketa AS (
    SELECT
        s.anketa_id,
        MAX(s.decision_date) AS claim_date
    FROM {{ source('raw', 'ins_sobitie_oracle') }} s
    WHERE s.anketa_id IS NOT NULL
    GROUP BY s.anketa_id
),

rastorg AS (
    SELECT
        r.tb_anketa AS anketa_id,
        r.tb_polis AS polis_id,
        MAX(r.tb_dateras) AS tb_dateras,
        SUM(COALESCE(r.tb_summa, 0)) AS tb_summa
    FROM {{ source('raw', 'ins_rastorg_oracle') }} r
    WHERE r.tb_anketa IS NOT NULL
      AND r.tb_dateras IS NOT NULL
      AND COALESCE(r.tb_summa, 0) <> 0
    GROUP BY
        r.tb_anketa,
        r.tb_polis
),

detailed_data AS (

    /* General + OSAGO new: pre-computed payment base (inline fifty / kurs) */
    SELECT
        CASE
            WHEN pb.pturi_id = 3 THEN 'OSAGO_NEW'
            ELSE 'GENERAL_INSURANCE'
        END AS source_type,

        pb.anketa_id,
        pb.anketa_id AS ank_id,
        pb.polis_id,
        pb.opl_data,
        pb.pym_date AS pay_date,

        cd.claim_date AS claim_date,
        COALESCE(cla.claim_value, 0) AS claim_value,

        pb.oplata,
        raw.f_ins_valtype(pb.opl_val) AS val,

        pb.kommis_summa,
        pb.kom_sum,

        CASE
            WHEN pb.user_surname = 'KSC.UZ'
             AND pb.user_name = 'WEB'
            THEN 'Website'

            WHEN pb.bank_k_id IS NOT NULL
            THEN 'Banks - ' || pb.bank_orgname

            WHEN pb.user_id IN (19588, 40791, 44788, 20879, 50788)
            THEN 'Banks - ' || TRIM(COALESCE(pb.user_surname, '') || ' ' || COALESCE(pb.user_name, ''))

            WHEN pb.user_id IN (
                19887, 20174, 20522, 20471, 20822, 20827, 20821,
                20819, 20538, 20732, 20463, 20729, 82800, 64788, 20282
            )
            THEN 'Marketplace'

            ELSE 'In-House'
        END AS channel,

        CASE
            WHEN pb.akt_type IN (0, 1, 2) THEN 'Agent'
            ELSE 'Internal'
        END AS agent_network,

        CASE
            WHEN pb.user_id IN (
                82793, 40791, 64788, 21741, 20845, 20829, 20827, 20732, 20731, 20729,
                20704, 20574, 20546, 20471, 20282, 20326, 20240, 20325, 19626, 20323,
                19998, 19887, 19768, 20322, 19459, 20318, 19417, 20317, 19366, 20504,
                20538, 20553, 20554, 20562, 20645, 20728, 20771, 20822, 20828, 20904,
                21410, 21409, 21740, 21739, 21774, 40795, 72788, 82792, 82794, 82795,
                19482, 19588, 19738, 20174, 20463, 20522, 20877, 19477,
                21367, 21405, 21751, 35788, 44788, 67790, 8280, 21362
            ) THEN 'API'
            ELSE 'Not API'
        END AS api_type,

        pb.fifty_zp,
        pb.fifty_dop,
        pb.fifty_director,

        pb.ins_otv,
        pb.ins_prem,
        raw.F_INS_PTURIKLASS(pb.pturi_id) AS klass,

        pb.oplsum,

        pb.bc_id,
        pb.pym_date,
        pb.oplata_id AS ins_id,
        pb.opl_type,
        pb.ins_div AS division_id,
        pb.pturi_id,
        pb.user_id,
        pb.owner_id

    FROM {{ ref('curated_partner_payment_base') }} pb

    LEFT JOIN claims_by_anketa cla
        ON cla.anketa_id = pb.anketa_id

    LEFT JOIN claim_dates_by_anketa cd
        ON cd.anketa_id = pb.anketa_id

    WHERE pb.is_osago_old = FALSE

    UNION ALL

    SELECT
        'OSAGO_OLD' AS source_type,

        t.tb_id AS anketa_id,
        t.tb_id AS ank_id,
        NULL AS polis_id,
        NULL AS opl_data,
        bc.pym_date AS pay_date,

        NULL AS claim_date,
        0 AS claim_value,

        COALESCE(o.tb_summa, 0) AS oplata,
        NULL AS val,

        NULL AS kommis_summa,
        NULL AS kom_sum,

        'In-House' AS channel,
        'Internal' AS agent_network,
        'Not API' AS api_type,

        NULL AS fifty_zp,
        NULL AS fifty_dop,
        NULL AS fifty_director,

        COALESCE(p.tb_summa, 0) AS ins_otv,
        COALESCE(p.tb_premia, 0) AS ins_prem,
        raw.F_INS_PTURIKLASS(3) AS klass,

        COALESCE(o.tb_summa, 0) AS oplsum,

        bc.ins_id AS bc_id,
        bc.pym_date,
        o.tb_id AS ins_id,
        o.tb_typepl AS opl_type,
        NULL AS division_id,
        3 AS pturi_id,
        NULL AS user_id,
        NULL AS owner_id

    FROM {{ source('raw', 'tb_anketa_oracle') }} t

    INNER JOIN {{ source('raw', 'tb_polis_oracle') }} p
        ON t.tb_id = p.tb_anketa

    INNER JOIN {{ source('raw', 'tb_oplata_oracle') }} o
        ON t.tb_id = o.tb_anketa

    INNER JOIN {{ source('raw', 'tb_avto_oracle') }} v
        ON t.tb_id = v.tb_anketa

    INNER JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON o.bc_id = bc.ins_id
       AND bc.status = 2

    WHERE o.tb_typepl = 1
      AND p.tb_status IN (2, 8)
      AND bc.pym_date >= DATE '2021-01-01'
),

prepared_base AS (
    SELECT
        DATE_TRUNC('month', d.pay_date) AS month,
        d.pturi_id,

        CASE
            WHEN UPPER(d.channel) LIKE '%BANKS%'
            THEN 'Banks'

            WHEN CAST(d.klass AS TEXT) ~ '(^|,)\s*3\s*(,|$)'
             AND UPPER(d.channel) NOT LIKE '%BANKS%'
            THEN '3'

            WHEN (
                    CAST(d.klass AS TEXT) ~ '(^|,)\s*8\s*(,|$)'
                 OR CAST(d.klass AS TEXT) ~ '(^|,)\s*9\s*(,|$)'
                 )
             AND UPPER(d.channel) NOT LIKE '%BANKS%'
            THEN '8,9'
        END AS filtr,

        CASE
            WHEN d.channel LIKE 'Banks - %' THEN 'Banks'
            ELSE d.channel
        END
        || ' - ' ||
        d.agent_network
        || ' - ' ||
        d.api_type AS channels,

        COALESCE(d.oplsum, 0) AS oplsum,
        COALESCE(d.kom_sum, 0) AS kom_sum,
        COALESCE(d.claim_value, 0) AS claim_value,

        COALESCE(d.fifty_zp, 0)
      + COALESCE(d.fifty_dop, 0)
      + COALESCE(d.fifty_director, 0) AS fifty,

        d.anketa_id,
        d.polis_id

    FROM detailed_data d
),

prepared AS (
    SELECT *
    FROM prepared_base
    WHERE filtr IS NOT NULL
),

main_grouped AS (
    SELECT
        month,
        pturi_id,
        filtr,
        channels,
        SUM(oplsum) AS oplsum,
        SUM(kom_sum) AS kom_sum,
        SUM(claim_value) AS claim_value,
        SUM(fifty) AS fifty,
        SUM(ras_value) AS ras_value
    FROM (
        SELECT
            p.month,
            p.pturi_id,
            p.filtr,
            p.channels,
            p.oplsum,
            p.kom_sum,
            p.claim_value,
            p.fifty,
            COALESCE(r.tb_summa, 0) AS ras_value
        FROM prepared p
        LEFT JOIN rastorg r
            ON r.anketa_id = p.anketa_id
           AND (r.polis_id = p.polis_id OR r.polis_id IS NULL)
    ) x
    GROUP BY
        month,
        pturi_id,
        filtr,
        channels
)

SELECT
    m.month,
    m.pturi_id,
    m.filtr,
    m.channels,
    m.oplsum,
    m.kom_sum,
    m.claim_value,
    m.fifty,
    m.ras_value
FROM main_grouped m

ORDER BY
    m.month,
    m.pturi_id,
    m.filtr,
    m.channels
