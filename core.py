# PPSQL: Python PostgreSQL
import os
import psycopg2 as pg 
import psycopg2.extras as ex
import pandas as pd
from .utils import timewrapper

def get_connection(db_configs):
    conn = pg.connect(**db_configs)
    cur = conn.cursor()
    return conn, cur

def _get_colnames(cur):
    """
    cur.description을 통해 table 열 이름 반환
    
    Args:
        cur: DB가 연결된 cursor
    
    Returns:
        colnames(list): 열 이름
    """
    colnames = [desc[0] for desc in cur.description]
    return colnames

def check_punc(query):
    """
    쿼리가 ";" 형태로 끝나는지 확인 후 없으면 마침 표식을 추가
    
    Args:
        query(str): 쿼리
    
    Returns:
        (str): 마침표가 확인/추가된 쿼리
    """
    return query if query.strip().endswith(';') else query + ';'


def _select(query, cur, n=-1):
    """
    SELECT 쿼리 실행 결과
    
    Args:
        query(str): 쿼리
        n(int): 출력할 결과의 수, -1이면 전체 출력
    
    Returns:
        (list) 쿼리 실행 결과
    """
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
    """
    conn.commit()이 필요한 쿼리 실행 함수

    Args:
        query (str): 쿼리
        cur (psycopg2.extensions.cursor): 연결된 cursor 객체
        conn (psycopg2.extensions.connection): 연결된 connection 객체
        values (tuple, optional): placeholder 활용할 때 value Defaults to None.
    """
    query = check_punc(query)
    cur.execute(query, values)
    conn.commit()

def pandas2tuples(df):
    """
    pandas data frame을 tuple 형태로 변환.
    tolist()를 통해서 numpy.dtype을 python native 형태로 변환
    
    Args:
        df(pandas.DataFrame): 변환할 pandas data frame
        
    Returns:
        (list): 변환된 tuple list
    """
    return [tuple(x.tolist()) for x in df.values]

def insert_from_tuples(query, tuples, cursor, conn):
    """
    tuple list를 입력으로 하여 table에 insert 수행

    Args:
        query (str): _description_
        tuples (list): tuple들의 list 형태
        cursor (psycopg2.extensions.cursor): 연결된 cursor 객체
        conn (psycopg2.extensions.connection): 연결된 connection 객체
    """
    query = check_punc(query)
    pg.extras.execute_values(cursor, query, tuples, template=None, page_size=1000)
    conn.commit()
    
    
class PyPostgreSql:
    """
    Python과 PostgreSQL 연동 클래스
    """
    def __init__(self, config, verbose=True):
        """
        Args:
            config (dict): DB 연결을 위한 config
            verbose (bool, optional): config 출력 여부. Defaults to True.
        """
        self.config = config
        self.conn = pg.connect(**config)
        self.cur = self.conn.cursor()

        if verbose:
            self.print_db_config()
            
    @timewrapper
    def select(self, query, n=-1, return_type='tuple'):
        """
        SELECT 수행

        Args:
            query (str): 쿼리(SELECT)
            n (int, optional): 반환할 결과의 개수. Defaults to -1. 만약 -1이면 전체 결과 반환
            return_type (str, optional): 반환 형태 'pandas'와 'tuple' 중 선택. Defaults to 'tuple'.

        Returns:
            (pandas.DataFrame or list ): SELECT 수행 결과. return_type에 따라 반환 형태 결정
        """
        result,colnames = _select(query, self.cur, n)
        
        if return_type == 'tuple': 
            return result, colnames
        elif return_type == 'pandas':
            return pd.DataFrame(result, columns=colnames)

    @timewrapper
    def insert(self, query, tuples=None, df=None):
        """
        tuple을 입력으로 하여 INSERT 수행

        Args:
            query (str): 쿼리(INSERT)
            tuples (list, optional): 추가할 값(tuple list). Defaults to None.
            df (pandas.DataFrame, optional): 추가할 값(data frame). Defaults to None.
        """
        if (not isinstance(df, type(None))) & isinstance(tuples, type(None)):
            tuples = pandas2tuples(df)
            
        insert_from_tuples(query, tuples, cursor=self.cur, conn=self.conn)
    
    @timewrapper
    def commit(self, query, values=None):
        """
        CREATE, INSERT, DELETE, UPDATE... 관련 명령 수행

        Args:
            query (str): 쿼리(CREATE, INSERT, DELETE, UPDATE, ...)
            values (tuple, optional): placeholder 형태로 명령 수행 시 값. Defaults to None.
        """
        commit_query(query, values=values, cur=self.cur, conn=self.conn)
    
    def print_db_config(self):
        """
        config 출력 함수
        """
        print("""
              DB connection config
              ---------------------------------
              DB address: {host}
              DB name: {dbname}
              user: {user}
              password: {password}
              port: {port}
              ---------------------------------
              """.format(**self.config))
    
    def close_conn(self):
        self.cur.close()
        self.conn.close()
        print('Connection is closed')



if __name__ == '__main__':
    # Define Configs for DB connection
    # DB_ADDRESS = "localhost"
    # DB_NAME = "analysis"
    # USER = 'postgres'
    # PASSWORD = '1234'
    # PORT = '3524'
    # db_configs = {'host': DB_ADDRESS, 'dbname': DB_NAME, 'user':USER, 'password': PASSWORD, 'port': PORT}
    
    # From config.yaml
    import yaml
    try:
        with open('config.yaml') as f:
            db_configs =yaml.safe_load(f)
    except:
        with open('./ppsql/config.yaml') as f:
            db_configs =yaml.safe_load(f)
        
    
    print(db_configs)
    
    # PyPostgreSQL class
    pp = PyPostgreSql(db_configs)
    
    # simple test
    print('simple test')
    print(pp.select('select school_2020 from district_2020 limit 3'))
    print(pp.select('select school_2020 from district_2020;', n=3))
    print('\n')
    
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
