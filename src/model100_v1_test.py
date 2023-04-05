def model100_v1_test(model100_start_test, model100_v1_data):
    excludes = ['time_to_pasc', 'pasc']
    cols = [c for c in model100_v1_data.columns if not c in excludes]
    
    df = model100_start_test.select(*cols)

    return df 