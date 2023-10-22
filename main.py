import os
os.chdir('../')
from ppsql import PyPostgreSql


pp = PyPostgreSql(verbose=True)

# Table Description
pp.get_table_description(table='test_table', schema='public', return_type='df')