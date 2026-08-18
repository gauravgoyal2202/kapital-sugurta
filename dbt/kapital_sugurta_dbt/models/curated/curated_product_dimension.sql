{{ config(materialized='view') }}

/*
  Product Dimension View
  ----------------------
  Enriches ins_pturi_oracle with:
  - insurance_type (+ ru / uz-Cyrl / uz-Latn): Compulsory / Voluntary
  - category:       Vertical name from ins_vertical_oracle
  - insurance_class_group: Sub-grouping from ins_pgroup1_oracle
  - insurance_subgroup:    Sub-grouping from ins_pgroup2_oracle

  Compulsory KOD_NUM list (per client): 3, 108, 123, 210, 211, 218, 401, 402, 418
  All other values (including NULL, blank, non-numeric) = Voluntary
*/

SELECT
    t.ins_id                                                AS pturi_id,
    t.polis_name_rus                                        AS product_name,
    t.polis_name_rus                                        AS product_name_ru,
    t.polis_name                                            AS product_name_uz,
    t.polis_name                                            AS product_name_uz_latn,
    t.kod_num::TEXT                                         AS product_code,

    {{ insurance_type_en('t.kod_num') }}                    AS insurance_type,
    {{ insurance_type_ru_from_kod_num('t.kod_num') }}       AS insurance_type_ru,
    {{ insurance_type_uz_cyrl_from_kod_num('t.kod_num') }}  AS insurance_type_uz_cyrl,
    {{ insurance_type_uz_latn_from_kod_num('t.kod_num') }}  AS insurance_type_uz_latn,

    COALESCE(v.name1, 'Unclassified')                       AS category,
    COALESCE(v.name1, 'Unclassified')                       AS category_ru,
    COALESCE(v.name2, 'Unclassified')                       AS category_uz,
    COALESCE(v.name3, 'Unclassified')                       AS category_uz_latn,

    g1.name1                                                AS insurance_class_group,
    g1.name2                                                AS insurance_class_group_uz,
    g2.name1                                                AS insurance_subgroup,
    g2.name2                                                AS insurance_subgroup_uz,

    t.vertical                                              AS vertical_id,
    t.group1                                                AS group1_id,
    t.group2                                                AS group2_id,
    t.active                                                AS is_active,
    t.import                                                AS import_flag

FROM {{ source('raw', 'ins_pturi_oracle') }} t
LEFT JOIN {{ source('raw', 'ins_vertical_oracle') }}  v  ON v.ins_id  = t.vertical
LEFT JOIN {{ source('raw', 'ins_pgroup1_oracle') }}   g1 ON g1.ins_id = t.group1
LEFT JOIN {{ source('raw', 'ins_pgroup2_oracle') }}   g2 ON g2.ins_id = t.group2
