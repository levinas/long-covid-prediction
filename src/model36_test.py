def model36_test(model36_start_test, model36_data):
    excludes = ['time_to_pasc', 'pasc']
    cols = [c for c in model36_data.columns if not c in excludes]
    
    df = model36_start_test.select(*cols)

    return df 