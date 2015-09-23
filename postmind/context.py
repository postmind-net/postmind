__author__ = 'matthieu'

import sqlalchemy as sa
import base64
import os
import json
from prettytable import PrettyTable
from postmind import pg
import uuid
import pandas as pd
from sqlalchemy.sql.expression import func
import logging
from logging.handlers import RotatingFileHandler
import numpy as np

from .column import Column
from .table import Table
from utils import gen_table_name, pgapply

queries_templates = {
    "column": {
        "head": "select {column} from {table} limit {n};",
        "all": "select {column} from {table};",
        "unique": "select distinct {column} from {table};",
        "sample": "select {column} from {table} order by random() limit {n};"
    },
    "table": {
        "select": "select {columns} from {table};",
        "head": "select * from {table} limit {n};",
        "all": "select * from {table};",
        "unique": "select distinct {columns} from {table};",
        "sample": "select * from {table} order by random() limit {n};"
    },
    "system": {
        "schema_for_table": """
                select
                    table_name
                    , column_name
                    , udt_name
                from
                    information_schema.columns
                where
                    table_name = {table}
                """,
        "schema_no_system": """
                select
                    table_name
                    , column_name
                    , udt_name
                from
                    information_schema.columns
                where
                    table_schema not in ('information_schema', 'pg_catalog')
                """,
        "schema_with_system": """
                select
                    table_name
                    , column_name
                    , udt_name
                from
                    information_schema.columns;
                """,
        "schema_specified": """
                select
                    table_name
                    , column_name
                    , udt_name
                from
                    information_schema.columns
                where table_schema in (%s);
                """,
        "foreign_keys_for_table": """
            SELECT
                kcu.column_name
                , ccu.table_name AS foreign_table_name
                , ccu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='%s';
        """,
        "foreign_keys_for_column": """
            SELECT
                kcu.column_name
                , ccu.table_name AS foreign_table_name
                , ccu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='%s' and kcu.column_name = '%s';
        """,
        "ref_keys_for_table": """
            SELECT
                ccu.column_name
                , kcu.table_name AS foreign_table_name
                , kcu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY' AND ccu.table_name='%s';
        """
    }
}

__all__ = ['PostmindContext']

logger = logging.getLogger('pom')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
file_handler = RotatingFileHandler('pom.log', 'a', 1000000, 1)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)





class TableSet(object):
    """
    Set of Tables. Used for displaying search results in terminal/ipython notebook.
    """
    def __init__(self, tables):
        for tbl in tables:
            setattr(self, tbl.name, tbl)
        self.tables = tables

    def __getitem__(self, i):
        return self.tables[i]

    def _tablify(self):
        tbl = PrettyTable(["Table", "Columns"])
        tbl.align["Table"] = "l"
        tbl.align["Columns"] = "l"
        for table in self.tables:
            column_names = [col.name for col in table._columns]
            column_names = ", ".join(column_names)
            pretty_column_names = ""
            for i in range(0, len(column_names), 80):
                pretty_column_names += column_names[i:(i+80)] + "\n"
            pretty_column_names = pretty_column_names.strip()
            tbl.add_row([table.name, pretty_column_names])
        return tbl

    def __repr__(self):
        tbl = str(self._tablify())
        return tbl

    def _repr_html_(self):
        return self._tablify().get_html_string()

class ColumnSet(object):
    """
    Set of Columns. Used for displaying search results in terminal/ipython
    notebook.
    """
    def __init__(self, columns):
        self.columns = columns

    def __getitem__(self, i):
        return self.columns[i]

    def _tablify(self):
        tbl = PrettyTable(["Table", "Column Name", "Type"])
        tbl.align["Table"] = "l"
        tbl.align["Column"] = "l"
        tbl.align["Type"] = "l"
        for col in self.columns:
            tbl.add_row([col.table, col.name, col.type])
        return tbl

    def __repr__(self):
        tbl = str(self._tablify())
        return tbl

    def _repr_html_(self):
        return self._tablify().get_html_string()

class PostmindContext(object):
    def __init__(self, uri=None, profile="default"):
        if uri is None:
            self.load_credentials(profile)
        else:
            self.uri = uri
        self._query_templates = queries_templates

        self.con = sa.create_engine(self.uri)

        self.tables = TableSet([])
        self.refresh_schema()

    def __str__(self):
        return "DB[{uri}]".format(uri=self.uri)

    def __repr__(self):
        return self.__str__()

    def __delete__(self):
        del self.con

    def apply(self, fun, *args, **kwargs):
        q = pgapply(self, fun, *args, **kwargs)
        return Table(self, gen_table_name(), q)

    def load_credentials(self, profile="default"):
        user = os.path.expanduser("~")
        f = os.path.join(user, ".db.py_" + profile)
        if os.path.exists(f):
            raw_creds = open(f, 'rb').read()
            raw_creds = base64.decodestring(raw_creds).decode('utf-8')
            creds = json.loads(raw_creds)
            self.uri = creds.get('uri')
        else:
            raise Exception("Credentials not configured!")

    def save_credentials(self, profile="default"):
        if self.filename:
            db_filename = os.path.join(os.getcwd(), self.filename)
        else:
            db_filename = None

        user = os.path.expanduser("~")
        dotfile = os.path.join(user, ".db.py_" + profile)
        creds = {
            "uri": self.uri
        }
        with open(dotfile, 'wb') as f:
            data = json.dumps(creds)
            try:
                f.write(base64.encodestring(data))
            except:
                f.write(base64.encodestring(bytes(data, 'utf-8')))

    def setup(self):
        def _setup():
            import plpy

            init_queries = ["CREATE EXTENSION IF NOT EXISTS file_fdw;",
                            "CREATE SERVER IF NOT EXISTS csv_server FOREIGN DATA WRAPPER file_fdw;"]
            qws = {}
            for query in init_queries:
                ret = True
                try:
                    plpy.execute(query)
                except Exception, ex:
                    plpy.warning("Cannot execute query '" + query + "': " + ex.message)
                    ret = False
                qws[query] = ret
            return [json.dumps(qws)]
        return self.apply(_setup)

    def read_csv(self, file_name, **kwargs):
        def _read_csv(in_file_name, out_table_name, sep=",", header=True, infer_limit=1000, index=None, expr='*', **kwargs):
            import plpy

            columns = []
            columns_types = []
            return columns, columns_types
        return self.apply(_read_csv, file_name, **kwargs)

    def find_table(self, search):
        tables = []
        for table in self.tables:
            if glob.fnmatch.fnmatch(table.name, search):
                tables.append(table)
        return TableSet(tables)

    def find_column(self, search, data_type=None):
        if isinstance(data_type, str):
            data_type = [data_type]
        cols = []
        for table in self.tables:
            for col in vars(table):
                if glob.fnmatch.fnmatch(col, search):
                    if data_type and isinstance(getattr(table, col), Column) and getattr(table, col).type not in data_type:
                        continue
                    if isinstance(getattr(table, col), Column):
                        cols.append(getattr(table, col))
        return ColumnSet(cols)

    def _assign_limit(self, q, limit=1000):
        # postgres, mysql, & sqlite
        if self.dbtype in ["postgres", "redshift", "sqlite", "mysql"]:
            if limit:
                q = q.rstrip().rstrip(";")
                q = "select * from ({q}) q limit {limit}".format(q=q, limit=limit)
            return q
        # mssql
        else:
            if limit:
                q = "select top {limit} * from ({q}) q".format(limit=limit, q=q)
            return q

    def query(self, q, limit=None):
        """
        Query your database with a raw string.

        Parameters
        ----------
        q: str
            Query string to execute
        limit: int
            Number of records to return

        Examples
        --------
        >>> from db import DemoDB
        >>> db.query("select * from Track")
           TrackId                                     Name  AlbumId  MediaTypeId  \
        0        1  For Those About To Rock (We Salute You)        1            1
        1        2                        Balls to the Wall        2            2
        2        3                          Fast As a Shark        3            2

           GenreId                                           Composer  Milliseconds  \
        0        1          Angus Young, Malcolm Young, Brian Johnson        343719
        1        1                                               None        342562
        2        1  F. Baltes, S. Kaufman, U. Dirkscneider & W. Ho...        230619

              Bytes  UnitPrice
        0  11170334       0.99
        1   5510424       0.99
        2   3990994       0.99
        ...
        >>> db.query("select * from Track", limit=10)
           TrackId                                     Name  AlbumId  MediaTypeId  \
        0        1  For Those About To Rock (We Salute You)        1            1
        1        2                        Balls to the Wall        2            2
        2        3                          Fast As a Shark        3            2
        3        4                        Restless and Wild        3            2
        4        5                     Princess of the Dawn        3            2
        5        6                    Put The Finger On You        1            1
        6        7                          Let's Get It Up        1            1
        7        8                         Inject The Venom        1            1
        8        9                               Snowballed        1            1
        9       10                               Evil Walks        1            1

           GenreId                                           Composer  Milliseconds  \
        0        1          Angus Young, Malcolm Young, Brian Johnson        343719
        1        1                                               None        342562
        2        1  F. Baltes, S. Kaufman, U. Dirkscneider & W. Ho...        230619
        3        1  F. Baltes, R.A. Smith-Diesel, S. Kaufman, U. D...        252051
        4        1                         Deaffy & R.A. Smith-Diesel        375418
        5        1          Angus Young, Malcolm Young, Brian Johnson        205662
        6        1          Angus Young, Malcolm Young, Brian Johnson        233926
        7        1          Angus Young, Malcolm Young, Brian Johnson        210834
        8        1          Angus Young, Malcolm Young, Brian Johnson        203102
        9        1          Angus Young, Malcolm Young, Brian Johnson        263497

              Bytes  UnitPrice
        0  11170334       0.99
        1   5510424       0.99
        2   3990994       0.99
        3   4331779       0.99
        4   6290521       0.99
        5   6713451       0.99
        6   7636561       0.99
        7   6852860       0.99
        8   6599424       0.99
        9   8611245       0.99
        >>> q = '''
        SELECT
          a.Title
          , t.Name
          , t.UnitPrice
        FROM
          Album a
        INNER JOIN
          Track t
            on a.AlbumId = t.AlbumId;
        '''
        >>> db.query(q, limit=10)
                                           Title  \
        0  For Those About To Rock We Salute You
        1                      Balls to the Wall
        2                      Restless and Wild
        3                      Restless and Wild
        4                      Restless and Wild
        5  For Those About To Rock We Salute You
        6  For Those About To Rock We Salute You
        7  For Those About To Rock We Salute You
        8  For Those About To Rock We Salute You
        9  For Those About To Rock We Salute You

                                              Name  UnitPrice
        0  For Those About To Rock (We Salute You)       0.99
        1                        Balls to the Wall       0.99
        2                          Fast As a Shark       0.99
        3                        Restless and Wild       0.99
        4                     Princess of the Dawn       0.99
        5                    Put The Finger On You       0.99
        6                          Let's Get It Up       0.99
        7                         Inject The Venom       0.99
        8                               Snowballed       0.99
        9                               Evil Walks       0.99
        """
        if limit==False:
            pass
        else:
            q = self._assign_limit(q, limit)
        return pd.io.sql.read_sql(q, self.con)

    def query_from_file(self, filename, limit=None):
        """
        Query your database from a file.

        Parameters
        ----------
        filename: str
            A SQL script

        Examples
        --------
        >>> from db import DemoDB
        >>> q = '''
        SELECT
          a.Title
          , t.Name
          , t.UnitPrice
        FROM
          Album a
        INNER JOIN
          Track t
            on a.AlbumId = t.AlbumId;
        '''
        >>> with open("myscript.sql", "w") as f:
        ...    f.write(q)
        ...
        >>> db.query_from_file(q, limit=10)
                                           Title  \
        0  For Those About To Rock We Salute You
        1                      Balls to the Wall
        2                      Restless and Wild
        3                      Restless and Wild
        4                      Restless and Wild
        5  For Those About To Rock We Salute You
        6  For Those About To Rock We Salute You
        7  For Those About To Rock We Salute You
        8  For Those About To Rock We Salute You
        9  For Those About To Rock We Salute You

                                              Name  UnitPrice
        0  For Those About To Rock (We Salute You)       0.99
        1                        Balls to the Wall       0.99
        2                          Fast As a Shark       0.99
        3                        Restless and Wild       0.99
        4                     Princess of the Dawn       0.99
        5                    Put The Finger On You       0.99
        6                          Let's Get It Up       0.99
        7                         Inject The Venom       0.99
        8                               Snowballed       0.99
        9                               Evil Walks       0.99
        """
        return self.query(open(filename).read(), limit)

    def refresh_schema(self):
        meta = sa.MetaData()
        meta.reflect(bind=self.con)
        tables = []
        for table in meta.tables:
            tables.append(Table(self, table, meta.tables[table]))
        from sqlalchemy import create_engine, inspect

        ins = inspect(self.con)
        for table in ins.get_foreign_table_names():
            cols = ins.get_columns(table)
            ocols = map(lambda x: sa.Column(x["name"], x["type"]), cols)
            tables.append(Table(self, table, sa.Table(table, sa.MetaData(), *ocols)))
        self.tables = TableSet(tables)

    def _try_command(self, cmd):
        try:
            self.cur.execute(cmd)
        except Exception as e:
            print ("Error executing command:")
            print ("\t '{0}'".format(cmd))
            print ("Exception: {0}".format(e))
            self.con.rollback()

    def textFile(self, path, sep=","):
        return self.apply(pg.csv.mount_csv, path, "tmp", sep=sep)

    def execute(self, *args, **kwargs):
        return self.con.execute(*args, **kwargs)

    def read_sql(self, query):
        return pd.io.sql.read_sql(query, self.con)

    def to_sql(self, qsa):
        from sqlalchemy.dialects import postgresql

        data = qsa
        if not data.is_selectable:
             data = sa.select([data])
        c = data.compile(dialect=postgresql.dialect())
        return self.con.raw_connection().cursor().mogrify(str(c), c.params)


def list_profiles():
    """
    Lists all of the database profiles available

    Examples
    --------
    >>> from db import list_profiles
    >>> list_profiles()
    {'demo': {u'dbname': None,
      u'dbtype': u'sqlite',
      u'filename': u'/Users/glamp/repos/yhat/opensource/db.py/db/data/chinook.sqlite',
      u'hostname': u'localhost',
      u'password': None,
      u'port': 5432,
      u'username': None},
     'muppets': {u'dbname': u'muppetdb',
      u'dbtype': u'postgres',
      u'filename': None,
      u'hostname': u'muppets.yhathq.com',
      u'password': None,
      u'port': 5432,
      u'username': u'kermit'}}
    """
    profiles = {}
    user = os.path.expanduser("~")
    for f in os.listdir(user):
        if f.startswith(".db.py_"):
            profile = os.path.join(user, f)
            profile = json.loads(base64.decodestring(open(profile).read()))
            profiles[f[7:]] = profile
    return profiles


def remove_profile(name, s3=False):
    """
    Removes a profile from your config
    """
    user = os.path.expanduser("~")
    if s3==True:
        f = os.path.join(user, ".db.py_s3_" + name)
    else:
        f = os.path.join(user, ".db.py_" + name)
    try:
        try:
            open(f)
        except:
            raise Exception("Profile '{0}' does not exist. Could not find file {1}".format(name, f))
        os.remove(f)
    except Exception as e:
        raise Exception("Could not remove profile {0}! Excpetion: {1}".format(name, e))


