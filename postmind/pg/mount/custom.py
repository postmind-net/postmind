__author__ = 'matthieu'

from multicorn import ForeignDataWrapper
from sklearn.feature_extraction import FeatureHasher
import pandas as pd
import numpy as np
import logging
import json
import sys


class PandasFDW(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(PandasFDW, self).__init__(options, columns)
        sys.argv = []
        self.columns = columns
        self.options = options
        self.logger = logging.getLogger('PandasFDW')
        hdlr = logging.FileHandler('/tmp/fdw.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr)
        self.logger.setLevel(logging.INFO)
        self.logger.info("PandasFDW options %s" %self.options)

    def execute(self, quals, columns):
        import pandas as pd

        args = json.loads(self.options.get("args", "{}"))
        for X_num, X_hash, X_meta in self.read_csv(self.options["path"], **args):
            for num, txt, meta in zip(X_num.values, X_hash.values, X_meta.to_dict("records")):
                rec = meta
                rec["num"] = '{%s}' %str(num.astype(float).tolist())[1:-1]
                rec["txt"] = '{%s}' %(str(txt.tolist())[1:-1])
                yield rec

    def read_csv(self, file_name, **kwargs):
        import IPython
        if 'chunksize' not in kwargs:
            kwargs['chunksize'] = 100000
        if 'meta' in kwargs:
            meta_cols = kwargs['meta']
            kwargs.pop('meta')
        else:
            meta_cols = None
#        kwargs["engine"] = "python"
        self.logger.info("read_csv options %s %s" %(file_name, str(kwargs)))
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
