# typical runtime: 4m;  output shape: 2 x 32 

def curated_bundles():
    selected_bundle_dict = get_selected_bundle_dict()
    pandas_bundle = pd.DataFrame.from_dict(selected_bundle_dict, orient='index').reset_index()
    pandas_bundle.columns = ['bundle_id', 'bundle_name']
    return spark.createDataFrame(pandas_bundle)
