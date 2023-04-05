# Records for shots are often duplicated on different days, especially
# in the procedures table for site 406. For that site, if shots are
# less than 21 days apart, use the earlier date and drop the latter.


from pyspark.sql.window import Window
import pyspark.sql.functions as f


def vax_deduplicated(distinct_vax_person):

    ################################################################################
    # 1. Resolve same day vaccinations with conflicting types. If one is null, use #
    #    the other. With multiple valid types, make null.                          #
    ################################################################################

    # Filter down to unique combinations of person, day, and vaccine type then drop
    # null values.
    vax_types = distinct_vax_person.dropDuplicates(
        ['person_id', 'vax_date', 'vax_type']
    ).filter(
        "vax_type is not NULL"
    )

    # Count number of types per person and day
    w = Window.partitionBy('person_id', 'vax_date')
    count_type = vax_types.select(
        'person_id', 
        'vax_date',
        'vax_type',
        f.count('person_id').over(w).alias('n')
    )

    # Drop rows with multiple values so they end up null after future join
    vax_types = count_type.filter(
        count_type.n == 1
    ).drop('n')

    # Drop original vax_type and merge this new one back into dataframe
    df = distinct_vax_person.drop(
        'vax_type'
    ).join(vax_types, on=['person_id', 'vax_date'], how='left')

    ################################################################################
    # 2. Deduplicate vaccines that are too close to be reasonable. Site 406 has    #
    #    extra issues due to using procedures table, so be more aggressive there.  #
    ################################################################################

    # Window by person_id
    w = Window.partitionBy('person_id').orderBy('vax_date')
    
    # Get difference between each shot in days
    df = df.withColumn(
        'lag_date', f.lag('vax_date', default='2000-01-01').over(w)
    ).withColumn(
        'date_diff', f.datediff('vax_date', 'lag_date')
    )

    # For site 406, filter if less than 14. For everyone else, filter if less than 5
    # df = df.filter(
    #     (
    #         (df.data_partner_id == 406) & (df.date_diff >= 14)
    #     ) | (
    #         (df.data_partner_id != 406) & (df.date_diff >= 5)
    #     )
    # )
    df = df.filter(df.date_diff >= 5)

    return df