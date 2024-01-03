import sqlalchemy
import textwrap
from sqlalchemy import *

from models import engine

connect_eco_string = engine.get_connect_string()

db_eco = create_engine(connect_eco_string, echo=True)


def run_sql_file(sqlfile):

    connect_eco_string = engine.get_connect_string()

    db_eco = create_engine(connect_eco_string, echo=True)

    try:

        with open(sqlfile, 'r', encoding='utf-8') as s:
            lines = s.read()
            query_string = textwrap.dedent("""{}""".format(lines))

        db_eco.execute(sqlalchemy.text(query_string))

        return True

    except:
        return False
