__author__ = 'matthieu'

import pandas as pd
import numpy as np

def read_csv(file_name, **kwargs):
    if 'chunksize' not in kwargs:
        kwargs['chunksize'] = 10000
    if 'meta' in kwargs:
        meta_cols = kwargs['meta']
        kwargs.pop('meta')
    else:
        meta_cols = None
#        kwargs["engine"] = "python"
    frames = pd.read_csv(file_name, **kwargs)
    num_feature_cols = None
    txt_feature_cols = None
    for df in frames:
        if num_feature_cols == None:
            num_feature_cols = []
            txt_feature_cols = []
            for col in range(len(df.columns)):
                tp = df.dtypes[col]
                name = df.columns[col]
                if name in meta_cols:
                    continue
                if tp in [np.float64, np.int, np.bool]:
                    num_feature_cols.append(name)
                else:
                    txt_feature_cols.append(name)
            print "# columns", [(i, j) for (i, j) in enumerate(df.columns)]
            print ""
            print "Numeric features", num_feature_cols
            print ""
            print "Text features", txt_feature_cols
            print ""
            print "Meta cols", meta_cols
        num_df = df[num_feature_cols]
        txt_df = df[txt_feature_cols]
        txt_df.fillna("", inplace=True)
        num_df.fillna(0, inplace=True)
        if meta_cols != None:
            meta_df = df[meta_cols]
            meta_df.columns = meta_df.columns.map(str.lower)
        else:
            meta_df = None
        yield num_df, txt_df, meta_df
