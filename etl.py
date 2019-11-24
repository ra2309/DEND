import glob
import os
import psycopg2
import pandas as pd
from sql_queries import *

def file_finder(path,ext):
    types = (ext,ext.upper())
    ex = []
    for typ in types:
        ex_files = glob.glob(path+'*'+typ)
        if ex_files != []:
            ex.append(ex_files)
        ex_files = glob.glob(path+'*/*'+typ)
        if ex_files != []:
            ex.append(ex_files)
    return ex



        

def process_expl_files(cur, filepath):
    """
    Description:
    This file will process song files into song and artist tables.
    Input:
    cur: pointer to database cursor
    filepath: location of data files for songs
    """
    rootDir = filepath
    wellDir = os.listdir(rootDir)
    las = []
    pdf = []
    tif = []
    mydicts = []
    for well in wellDir:
        well_path = rootDir+'/'+well
        logDir = os.listdir(well_path)
        for log in logDir:
            log_name = log[3:].strip()
            log_path = well_path + '/' + log + '/'
            las=file_finder(log_path,'.las')
            pdf=file_finder(log_path,'.pdf')
            tif=file_finder(log_path,'.tif')
            for l in las:
                mydict = {'well':well,'log_name':log_name,'file_location':l[0],'extension':'las'}
                mydicts.append(mydict)
            for p in pdf:
                mydict = {'well':well,'log_name':log_name,'file_location':p[0],'extension':'pdf'}
                mydicts.append(mydict)
            for t in tif:
                mydict = {'well':well,'log_name':log_name,'file_location':t[0],'extension':'tif'}
                mydicts.append(mydict)
    expl = pd.DataFrame.from_dict(mydicts).values[0].tolist()
    cur.execute(expl_table_insert, expl)

def process_drill_files(cur, filepath):
    """
    Description:
    Processes log files and fill user, time and songplay tables
    Input:
    cur: pointer to database cursor
    filepath: location of log files
    """
    rootDir = filepath
    fieldDir = os.listdir(rootDir)
    witsml = []
    mydicts = []
    for field in fieldDir:
        field_path = rootDir+'/'+field
        wellDir = os.listdir(field_path)
        for well in wellDir:
            if well == '_wellInfo':
                well_name = '0'
            else:
                well_name = well
            well_path = field_path + '/' + well + '/'
            xml=file_finder(well_path,'.xml')
            for x in xml:
                mydict = {'well':field,'wellbore':well_name,'xml':x[0]}
                mydicts.append(mydict)
    drill = pd.DataFrame.from_dict(mydicts).values[0].tolist()
    cur.execute(drill_table_insert, drill)
def process_prod_file(cur,filepath):
    prod = pd.read_excel(filepath)
    prod.drop(['NPD_WELL_BORE_CODE','NPD_WELL_BORE_NAME','NPD_FIELD_CODE','NPD_FACILITY_CODE','AVG_CHOKE_UOM'],axis=1,inplace=True)
    df = prod
    prod = prod.values[0].tolist()
    
    cur.execute(prod_table_insert,prod)
    for index, row in df.iterrows():
        cur.execute(fact_select, (row.WELL_BORE_CODE,))
        results = cur.fetchone()
        if results:
            prodid, explid,drillid = results
        else:
            prodid, explid,drillid = None, None,None

    # insert songplay record
        fact_data = (row.WELL_BORE_CODE,row.BORE_OIL_VOL,explid,drillid)
        cur.execute(fact_table_insert, fact_data)
        

def process_data(cur, conn, filepath, func):
    """
    Description:
    Function that will use above helper functions to process files
    Input:
    cur: database cursor
    conn: connection to database
    filepath: location of files
    func: helper function to be used
    """
    func(cur, filepath)
    conn.commit()


def main():
    conn = psycopg2.connect("host=denddb.postgres.database.azure.com dbname=oildb user=rmamnk@denddb password=NPrhYs17U4")
    cur = conn.cursor()

    process_data(cur, conn, filepath='/data/welllogs/Well_logs_pr_WELL', func=process_expl_files)
    process_data(cur, conn, filepath='/mnt/denddiag/WITSML Realtime drilling data', func=process_drill_files)
    process_data(cur, conn, filepath='/mnt/denddiag/Volve production data.xlsx', func=process_prod_file)
     
    conn.close()


if __name__ == "__main__":
    main()