def distinct_vax_person(concept_set_members, 
                        drug_exposure, 
                        # manifest_safe_harbor,
                        procedure_occurrence):
    subquery1 = concept_set_members.alias("cs").join(
        drug_exposure.alias("de"), col("cs.concept_id") == col("de.drug_concept_id"), "inner"
    # ).join(
    #     manifest_safe_harbor.alias("m"), col("de.data_partner_id") == col("m.data_partner_id"), "inner"
    ).filter(
        (col("cs.codeset_id") == 600531961)
        & (col("de.drug_exposure_start_date").isNotNull())
        & (datediff(col("de.drug_exposure_start_date"), to_date(lit("2018-12-20"))) > 0)
        # & (datediff(col("de.drug_exposure_start_date"), col("m.run_date")) < 356 * 2)
        & (
            (~col("de.drug_type_concept_id").isin([38000177, 32833, 38000175, 32838, 32839]))
            | ((col("de.drug_type_concept_id") == 38000177) 
               # & (col("m.cdm_name") == "ACT")
               )
        )
    ).select(
        col("de.person_id"),
        # col("de.data_partner_id"),
        col("de.drug_exposure_start_date"),
        # col("de.drug_source_concept_name"),
        col("de.drug_concept_id"),
        when(col("cs.concept_id").isin([702677, 702678, 724907, 37003432, 37003435, 37003436]), "pfizer")
        .when(col("cs.concept_id").isin([724906, 37003518]), "moderna")
        .when(col("cs.concept_id").isin([702866, 739906]), "janssen")
        .when(col("cs.concept_id") == 724905, "astrazeneca")
        .otherwise(None).alias("vax_type")
    ).distinct().select(
        col("de.person_id"),
        # col("de.data_partner_id"),
        col("de.drug_exposure_start_date").alias("vax_date"),
        col("vax_type")
    ).distinct()

    subquery2 = procedure_occurrence.alias("po"
    # ).join(
    #     manifest_safe_harbor.alias("m"), col("po.data_partner_id") == col("m.data_partner_id"), "inner"
    ).filter(
        col("po.procedure_concept_id").isin([766238, 766239, 766241])
        # & (col("po.data_partner_id") == 406)
        & (datediff(col("po.procedure_date"), to_date(lit("2018-12-20"))) > 0)
        # & (datediff(col("po.procedure_date"), col("m.run_date")) < 356 * 2)
        & (col("po.procedure_date").isNotNull())
    ).select(
        col("po.person_id"),
        # col("po.data_partner_id"),
        col("po.procedure_date").alias("vax_date"),
        when(col("po.procedure_concept_id") == 766238, "pfizer")
        .when(col("po.procedure_concept_id") == 766239, "moderna")
        .when(col("po.procedure_concept_id") == 766241, "janssen")
        .otherwise(None).alias("vax_type")
    ).distinct()

    return subquery1.union(subquery2).select("person_id", "vax_date", "vax_type").distinct()