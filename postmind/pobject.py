__author__ = 'matthieu'
from psycopg2.extensions import QuotedString

from pg.ops import store_h5, store_bin
from .utils import pgapply

class PObject(object):
    data = None
    ctx = None

    def __init__(self, ctx, data):
        self.ctx = ctx
        self.data = data

    def to_sql(self):
        return self.ctx.to_sql(self.data)

    def to_hdf(self, name, file_name):
        sql = self.to_sql()
        self.ctx.execute(pgapply(self.ctx, store_h5, sql, name, file_name))

    def to_bin(self, file_name):
        sql = self.to_sql()
        return self.ctx.execute(pgapply(self.ctx, store_bin, sql, file_name))
