__author__ = 'matthieu'

def store_h5(query, name, file_name):
    import tables
    import plnumpy
    import pandas as pd

    res = plnumpy.from_table(query)
    with pd.HDFStore(file_name, "a", complevel=3, complib='blosc') as store:
        df = pd.DataFrame(res)
        store.put(name, df, format='table')
    return [1]


def store_bin(query, file_name, compress=3):
    import plnumpy
    import numpy as np
    import joblib

    res = plnumpy.from_table(query)
    joblib.dump(res, file_name, compress=compress)
    return [1]

