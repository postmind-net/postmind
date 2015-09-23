__author__ = 'matthieu'

import sqlalchemy as sa
from prettytable import PrettyTable
import pandas as pd
from sqlalchemy.sql.expression import func
import logging
import numpy as np
from joblib import Parallel, delayed
from .utils import pgapply, gen_table_name, cached_property, process_query
from pobject import PObject

class Column(PObject):
    def __init__(self, ctx, data, table, name=gen_table_name()):
        self.ctx = ctx
        self.data = data
        try:
            self.name = data.name
            self.type = data.type
        except:
            self.name = name
            self.type = sa.VARCHAR
        self.table = table
        self.logger = logging.getLogger('pom')

    def __repr__(self):
        tbl = PrettyTable(["Table", "Name", "Type", "Foreign Keys",
                           "Reference Keys"])
        tbl.add_row([self.table, self.name, self.type, self.foreign_keys_str(),
                     self.ref_keys_str()])
        return str(tbl)

    def __str__(self):
        return "Column({0})".format(self.name)

    def _repr_html_(self):
        tbl = PrettyTable(["Table", "Name", "Type"])
        tbl.add_row([self.table, self.name, self.type])
        return tbl.get_html_string()

    def foreign_keys_str(self):
        try:
            keys = []
            for k in self.data.foreign_keys:
                keys.append(str(k))
            return ', '.join(keys)
        except:
            return ''

    def ref_keys_str(self):
        try:
            keys = []
            for k in self.data.constraints:
                keys.append(str(k))
            return ', '.join(keys)
        except:
            return ''

    def head(self, n=6):
        return self.ctx.read_sql(sa.select([self.data], limit=n))

    def __getitem__(self, item):
        try:
            if type(item) is int and self.data.type.python_type is list:
                return Column(self.ctx,
                             sa.select([sa.text("%s[%d]" %(self.data.name, item))]).
                             select_from(self.table),
                              self.table, name="%s[%d]" %(self.data.name, item))
        except:
            raise Exception("Not an array type")

    def distinct_count(self, approx=True):
        query = self.data.alias(name="col")
        if approx:
            return self.ctx.read_sql(sa.select([sa.text("madlib.fmsketch_dcount(col)")]).
                                      select_from(query)).values[0][0]
        else:
            return self.ctx.read_sql(sa.select([sa.text("count(distinct col)")]).
                                      select_from(query)).values[0][0]

    def values_count(self, count=10):
        query = self.data.alias(name="col")
        vcount = sa.select([sa.text("madlib.mfvsketch_top_histogram(col, %s)::text"
                                                     %count)]).select_from(query)
        return self.ctx.read_sql(vcount).values[0][0]

    @cached_property
    def shape(self):
        try:
            if self.data.type.python_type is list:
                df = self.ctx.read_sql(sa.select([func.min(func.array_length(self.data, 1)).label("min"),
                                        func.max(func.array_length(self.data, 1)).label("max")]))
                return df.values[0]
        except:
            return 1

    @cached_property
    def ndims(self, limit=100):
        try:
            if self.data.type.python_type is list:
                df = self.ctx.read_sql(sa.select([func.max(func.array_ndims(self.data)).label("ndims")]))
                return df.values[0][0]
            else:
                return 0
        except Exception, ex:
            print ex
            return 0

    def summary(self, count=32, out_table=None, prange=None, pjob=4):
        if self.ndims > 0:
            if_exists = "replace"
            if out_table == None:
                out_table = "%s_%s_summary" %(self.table, self.name)
                out_table = out_table.replace("[", "_").replace("]", "_")
            def query(i):
                self.logger.info("Processing column %d (of %d)" %(i, self.shape[0]))
                query = self[i].data.alias(name="col")
                q1 = sa.select([sa.text("madlib.fmsketch_dcount(col) as count"),
                                sa.text("madlib.mfvsketch_top_histogram(col, %s) as top" %count)]).select_from(query)
                return [q1, i]
            if prange == None:
                prange = range(1, self.shape[0] + 1)
            queries = [query(i) for i in prange]
            dfs = Parallel(n_jobs=pjob)(delayed(process_query)(q) for q in queries)
            dfs = pd.concat(dfs)
            dfs.index = prange
            dfs["table"] = self.table
            dfs["column"] = self.name
            return dfs

    def percentiles(self, n=10):
        if self.ndims == 0:
            step = 1. / n
            pvalues = np.arange(step, 1, step)
            iquery = self.data.alias("col")
            query = sa.select([sa.text("percentile_disc(array[{pvalues}]) within group (order by {col})".
                                       format(pvalues=','.join(map(str, pvalues)), col=self.name))]).\
                select_from(self.table)
            return Column(self.ctx, query, self.table, "percentile")

    def collect(self):
        return self.ctx.read_sql(sa.select([self.data]))

    def to_csv(self, file_name, alias=None, compression=True):
        from sqlalchemy.dialects import postgresql

        data = self.data
        if data.is_selectable == False:
            data = sa.select([data])
        c = data.compile(dialect=postgresql.dialect())
        if compression:
            q = "COPY (%s) TO PROGRAM 'gzip > %s'" %(c, file_name)
        else:
            q = "COPY (%s) TO '%s'" %(c, file_name)
        q += " WITH (format csv, header false, DELIMITER '|')"
        self.ctx.execute(q, c.params)


