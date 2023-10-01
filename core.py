# Python PostgreSQL
import psycopg2 as pg 
import psycopg2.extras as ex
import pandas as pd


def get_connection(db_configs):
    conn = pg.connect(**db_configs)
    cur = conn.cursor()
    return conn, cur

def _get_colnames(cur):
    '''
    cur.description을 통해 table 열 이름 반환
    
    Args:
        cur: DB가 연결된 cursor
    
    Returns:
        (list): 열 이름
    '''
    colnames = [desc[0] for desc in cur.description]
    return colnames

def check_punc(query):
    return query if query.strip().endswith(';') else query + ';'


def _select(query, cur, n=-1):
    '''
    query(str): PostgreSQL query
    cur
    '''
    try:
        assert isinstance(n, int)
    except:
        raise ValueError("'n' must be integer")
    
    query = check_punc(query)
    
    cur.execute(query)
    if n == -1:
        result = cur.fetchall()
    elif n == 1:
        result = cur.fetchone()
    else:
        result = cur.fetchmany(n)
    colnames = _get_colnames(cur)
    return result, colnames
    
def commit_query(query, cur, conn, values=None):
    query = check_punc(query)
    cur.execute(query, values)
    conn.commit()

def pandas2tuples(pandas_df):
    '''
    pandas data frame -> tuple 형태로 변환.
    tolist()를 통해서 numpy.dtype을 python native 형태로 변환
    
    Args:
        pandas_df(pandas.DataFrame): 변환할 pandas data frame
        
    Returns:
        (list): 변환된 tuple list
    '''
    return [ tuple(x.tolist()) for x in pandas_df.values]

def insert_from_tuples(query, tuples, cursor, conn):
    '''
    Args:
        : pandas data frame
        
    '''
    query = check_punc(query)
    pg.extras.execute_values(cursor, query, tuples, template=None, page_size=1000)
    conn.commit()
    
    
class PyPostgreSql:
    def __init__(self, config, verbose=True):
        self.config = config
        self.conn = pg.connect(**config)
        self.cur = self.conn.cursor()

        if verbose:
            self.print_db_config()
            
            
    def select(self, query, n=-1, return_type='tuple'):
        result,colnames = _select(query, self.cur, n)
        
        if return_type == 'tuple': 
            return result, colnames
        elif return_type == 'pandas':
            return pd.DataFrame(result, columns=colnames)

    def commit(self, query, values=None):
        '''
        create, insert, update, delete
        '''
        commit_query(query, values=values, cur=self.cur, conn=self.conn)
    
    def insert(self, query, tuples=None, df=None):
        if (not isinstance(df, type(None))) & isinstance(tuples, type(None)):
            tuples = pandas2tuples(df)
            
        insert_from_tuples(query, tuples, cursor=self.cur, conn=self.conn)
            
            
    def print_db_config(self):
        print("""
                DB address: {}\n
                DB name: {}\n
              """.format(DB_ADDRESS, DB_NAME))
    
    def close_conn(self):
        self.cur.close()
        self.conn.close()
        print('Connection is closed')



if __name__ == '__main__':
    import pandas as pd
    
    # Configs for DB connection
    DB_ADDRESS = "localhost"
    DB_NAME = "analysis"
    USER = 'postgres'
    PASSWORD = '1234'
    PORT = '3524'
    db_configs = {'host': DB_ADDRESS, 'dbname': DB_NAME, 'user':USER, 'password': PASSWORD, 'port': PORT}
    
    # PyPostgreSQL class
    pp = PyPostgreSql(db_configs)
    
    # simple test
    print(pp.select('select school_2020 from district_2020 limit 3;'))
    print(pp.select('select school_2020 from district_2020;', n=3))
    
    
    # (1) Create table
    pp.commit('drop table if exists test_table')
    pp.commit("create table test_table (id int, name varchar(30), age int)")
    print('Init table: ',pp.select("select * from test_table"))

    # (2) Insert Values
    # (2-1) SQL Query
    pp.commit("INSERT INTO test_table (id, name) VALUES (1, 'a'), (2, 'b')")
    print(pp.select('select * from test_table'))

    # (2-2) Using Pandas Data frame
    test_df = pd.DataFrame({'id': [4, 10], 'name': ['황구', '빡구'], 'age': [25,26]}) # data frame type input
    pp.insert(query='INSERT INTO test_table VALUES %s', df = test_df)
    print(pp.select('select * from test_table', return_type='pandas'))
    
    test_tuple = [(11, 'mang')] # tuple type input
    pp.insert(query='INSERT INTO test_table (id, name) VALUES %s', tuples=test_tuple)
    print(pp.select('select * from test_table', return_type='pandas'))
    
    
    # (3) Delete
    print('After deletion')
    pp.commit("DELETE FROM test_table WHERE id=1")
    print(pp.select('select * from test_table', return_type='pandas'))
    
    
    # (4) Update
    # pp.commit("UPDATE test_table set name='망' WHERE name='mang' ") # query type
    pp.commit("UPDATE test_table set name=%s WHERE name=%s", ('망', 'mang') )
    print(pp.select('select * from test_table', return_type='pandas'))
    
    # (5) close connection
    pp.close_conn()
