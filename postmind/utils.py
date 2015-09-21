__author__ = 'matthieu'

import uuid
import dill
import base64
import sqlalchemy as sa
import json
from sqlalchemy.sql.expression import func

def gen_table_name(prefix="tbl_"):
    return prefix + str(uuid.uuid4()).replace("-", "_")

class cached_property(object):
    """
    Descriptor (non-data) for building an attribute on-demand on first use.
    """
    def __init__(self, factory):
        """
        <factory> is called such: factory(instance) to build the attribute.
        """
        self._attr_name = factory.__name__
        self._factory = factory

    def __get__(self, instance, owner):
        # Build the attribute.
        attr = self._factory(instance)

        # Cache the value; hide ourselves.
        setattr(instance, self._attr_name, attr)

        return attr

def pgapply(ctx, fun, *args, **kwargs):

    otype = kwargs.pop("otype", "setof jsonb")
    metadata = kwargs.pop("meta", None)
    funname = fun.__name__
    query = """create or replace function {name} (inargs jsonb) returns {otype} as
$$
import sys
sys.argv = []
import base64
from json import loads, dumps
from postmind import pg

if '{name}' not in SD:
    import dill
    SD['{name}'] = dill.loads(base64.decodestring('''{code}'''))
args, kwargs = loads(inargs)

return SD['{name}'](*args, **kwargs)
$$ language plpythonu security definer;
""".format(name=funname, otype=otype, code=base64.encodestring(dill.dumps(fun)))
    ctx.execute(query)
#    query = "{name}('{args}')".format(name=funname, args=json.dumps((args, kwargs)))
#        return #Table(self.con, gen_table_name()
    query = func.__getattr__(funname)(json.dumps((args, kwargs)))
    if metadata == None:
        q = sa.select(['*']).select_from(query)
    else:
        q = sa.select(["'%s'::jsonb as meta" %json.dumps(metadata), '*']).select_from(query)
    return q

