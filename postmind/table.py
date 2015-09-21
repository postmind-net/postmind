__author__ = 'matthieu'

import sqlalchemy as sa
from prettytable import PrettyTable
import pandas as pd
from sqlalchemy.sql.expression import func

from .utils import *
from .column import Column
from .pobject import PObject

class Table(PObject):
    def __init__(self, ctx, name, data=None, columns=None):
        self.ctx = ctx
        self.name = name
        self.data = data

        self._columns = []
        self._columns_indexes = {}
        idx = 0

        cols = self.data.c if columns is None else columns
        for col in cols:
            self._columns.append(Column(self.ctx, col, self.name))
            attr = col.name
            if attr in ("name", "ctx", "data"):
                attr = "_" + col.name
            setattr(self, attr, self._columns[-1])
            self._columns_indexes[col.name] = idx
            idx += 1

    def _tablify(self):
        tbl = PrettyTable(["Column", "Type", "Foreign Keys", "Reference Keys"])
        tbl.align["Column"] = "l"
        tbl.align["Type"] = "l"
        tbl.align["Foreign Keys"] = "l"
        tbl.align["Reference Keys"] = "l"
        for col in self._columns:
            try:
                tp = col.type.python_type()
            except:
                tp = None
            tbl.add_row([col.name, tp, col.foreign_keys_str(), col.ref_keys_str()])
        return tbl

    def to_csv(self, file_name, alias="col", compression=True):
        from sqlalchemy.dialects import postgresql

        c = self.data.compile(dialect=postgresql.dialect())
        if compression:
            q = "COPY (%s) TO PROGRAM 'gzip > %s'" %(c, file_name)
        else:
            q = "COPY (%s) TO '%s'" %(c, file_name)
        q += " WITH (format csv, header false, DELIMITER '|')"
        self.ctx.execute(q, c.params)

    def __repr__(self):
        tbl = str(self._tablify())
        r = tbl.split('\n')[0]
        brk = "+" + "-"*(len(r)-2) + "+"
        title = "|" + self.name.center(len(r)-2) + "|"
        return brk + "\n" + title + "\n" + tbl

    def __str__(self):
        return "Table({0})".format(self.name)

    def _repr_html_(self):
        return self._tablify().get_html_string()

    def head(self, n=6):
        return self.ctx.read_sql(sa.select(['*'], limit=n).select_from(self.data.alias()))

    def union(self, tables):
        from sqlalchemy import union_all

        if type(tables) is list:
            datas = [self.data] + map(lambda x: x.data, tables)
        else:
            datas = [self.data, tables.data]
        return Table(self.ctx, gen_table_name(), union_all(*datas))#, self.data.c)

    def select(self, columns=None, **kwargs):
        name = gen_table_name(self.name)
        sort = kwargs.pop("sort", None)
        cols = []
        for col in columns:
            if type(col) is str:
                cols.append(self._columns[self._columns_indexes[col]].data)
            elif type(col) is sa.sql.functions.Function:
                cols.append(col)
            elif type(col) is Column:
                cols.append(col.data)
            else:
                cols.append(col)
        q = sa.select(cols, **kwargs)
        if type(sort) is sa.sql.elements.UnaryExpression:
            q = q.order_by(sort)
        return Table(self.ctx, name, q)

    def __getitem__(self, item):
        if type(item) == str:
            return self._columns[self._columns_indexes[item]]
        elif type(item) == int:
            return self._columns[item]
        elif type(item) == list:
            return self.select(columns=item)
        elif isinstance(item, slice):
            return self.slice(item.start, item.stop)

    def __len__(self):
        res = self.ctx.execute(self.data.count())
        return int(res.fetchall()[0][0])

    def map(self, fun, restype="json"):
        from pom.serializers import CloudPickleSerializer
        import base64

        Session = sessionmaker(bind=self.ctx)
        session = Session()

        funname = "myfun"#gen_table_name("fun_")
        ser = CloudPickleSerializer()
        query = """
create or replace function {name} (line {tablename})
 returns {restype} as
$$
import sys
sys.argv = []
import json

if '{name}' not in SD:
  import cPickle
  import base64
  SD['{name}'] = cPickle.loads(base64.decodestring('''{code}'''))

return SD['{name}'](line)
$$ language plpythonu security definer;
""".format(name=funname, tablename=self.name,
           code=base64.encodestring(ser.dumps(fun)),
           restype=restype)
        with self.ctx.connect() as conn:
            conn.execute(query)
        return Table(self.ctx, funname,
                     sa.select([sa.text("%s(%s.*)" %(funname, self.name))]).select_from(self.data))

    def collect(self):
        return self.ctx.read_sql(sa.select(['*']).select_from(self.data.alias()))


