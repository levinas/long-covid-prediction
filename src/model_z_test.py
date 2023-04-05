def model_z_test(model_z_start_test, model_z_data):
    excludes = ['time_to_pasc', 'pasc']
    cols = [c for c in model_z_data.columns if not c in excludes]
    
    df = model_z_start_test.select(*cols)

    return df  