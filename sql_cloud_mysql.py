import constants as ct
import sys
import pymysql 
import sqlalchemy
from sqlalchemy import create_engine;
import pandas as pd


def createGCPSQLEngine(*args):
    try:
        logger.info("Establishing connection to database")
        user=args[0]
        db=args[1]
        password=args[2]
        ip=args[3]
        logger=args[4]
        charset='UTF8'
        engine = create_engine('mysql+pymysql://{user}:{pw}@{ip}/{db}?charset={charset}'
                .format(user=user,
                        pw=password,
                        ip=ip,
                        db=db,
                        charset=charset),pool_recycle=600,pool_size=100, pool_timeout=600)
        logger.info("Connection established")
        return engine 
    except Exception as err:
        logger.info("Something went wrong establishing connection. Exiting program ...")
        logger.error(err)
        sys.exit(0)    

def upsertDataFrame(dataframe,database_name,table_name,primary_keys,connector,remove_not_updated,logger):
    '''
    dataframe: the dataframe to insert into database
    database_name: the name of your database where you are upserting the info
    table_name: name of the table where you are upserting the info
    primary_keys: primary keys of the table, to allow updating when duplicate keys
    connector: name of the connector
    remove_not_updated: after upserting, remove_not_updated to True removes all rows where last_updated is not max(last_updated)
    logger: logger
    '''
    try:
        dataframe=dataframe.reset_index(drop=True)
        logger.info("Starting the upsert of the table for dataframe:\n %s" %dataframe)
        last_update=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dataframe['last_update']=last_update
        dataframe=dataframeToString(dataframe,logger)
        dataframe_no_pks=dataframe.drop(columns=primary_keys)
        list_of_lists_inserts=dataframe.values.tolist()                     #List of values with pk for inserts
        list_of_lists_updates=dataframe_no_pks.values.tolist()              #List of values without pk for update
        list_of_tuples_inserts = list(map(tuple, list_of_lists_inserts))
        list_of_tuples_updates = list(map(tuple, list_of_lists_updates))
        list_of_columns_inst=dataframe.columns.values.tolist()              #Column names
        list_of_columns_updt=dataframe_no_pks.columns.values.tolist()       #Column names
        str_of_cols = ','.join(list_of_columns_inst)
        str_of_vals=''
        str_of_updt=''
        for i in range(len(list_of_tuples_inserts[0])-1):
            str_of_vals=str_of_vals+'%s,'
        str_of_vals=str_of_vals+'%s'

        for i in range(len(list_of_columns_updt)-1):
            str_of_updt=str_of_updt+list_of_columns_updt[i]+'=%s,'
        str_of_updt=str_of_updt+list_of_columns_updt[-1]+'=%s'
        
        for i in range(len(list_of_tuples_inserts)):
            insert_values=list_of_tuples_inserts[i]
            update_values=list_of_tuples_updates[i]
            sql="INSERT INTO "+database_name+"."+table_name+" ("+str(str_of_cols) +") VALUES ("+str(str_of_vals)+") ON DUPLICATE KEY UPDATE "+str(str_of_updt)
            connector.execute(sql,(*insert_values,*update_values))
        logger.info("Upserting finished. Deleting not updated data from  %s" %table_name)
        if (remove_not_updated==True):
            sql="DELETE FROM  "+database_name+"."+table_name+" WHERE last_update NOT LIKE %s"
            connector.execute(sql,last_update)
            logger.info("Data deleted from table %s" %table_name)
        return
    except Exception as err:
        logger.info("Something went wrong upserting data for table  %s. Exiting program ..." %table_name)
        logger.error(err)
        sys.exit(0)

def dataframeToString(dataframe,logger):
    try:
        logger.info("Adapting dataframe before inserting data into mysql")
        dataframe=dataframe.astype(str)
        dataframe=dataframe.replace(r'^\s*$', None, regex=True)
        dataframe=dataframe.replace({'NaT': None})
        dataframe=dataframe.replace({'nan': None})
        dataframe=dataframe.replace({np.nan: None})
        dataframe=dataframe.replace({'None': None})
        logger.info("All data has been converted to string")
        return dataframe
    except Exception as err:
        logger.info("Something went wrong converting data to string. Exiting program...")
        logger.error(err)
        sys.exit(0)

def simpleQueryExample():
    engine=createGCPSQLEngine(ct.sql_user,ct.sql_db1,ct.sql_password,ct.sql_ip)
    conn = engine.connect(close_with_result=True)
    result = conn.execute('show databases')
    for row in result:
        print(row)
    #df=pd.read_sql(sql,conn)
    #print("dataframe ",df)
    return df

