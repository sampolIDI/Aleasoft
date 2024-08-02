import pandas as pd
import seaborn as sns
import json
import pandas as pd
import seaborn as sns
import xlrd
from datetime import date, timedelta,datetime
import matplotlib.pyplot as plt
import os
from sqlalchemy import create_engine
import pymysql.cursors


#LEE ULTIMA FECHA
dialect='mysql+pymysql://dams:G3M3L0@172.30.1.106:3306/bd_parcbit'
sqlEngine=create_engine(dialect)
dbConnection = sqlEngine.connect()
sql="select max(fecha) from aleasoft.motoresparcbit"
fecha=pd.read_sql(sql, dbConnection)
dia=datetime.strptime(fecha.loc[0,'max(fecha)'],"%Y-%m-%d")+timedelta(days=1)
#CARGA MOTORES
while dia<datetime.now():
    dialect='mysql+pymysql://dams:G3M3L0@172.30.1.106:3306/bd_parcbit'
    sqlEngine=create_engine(dialect)
    dbConnection = sqlEngine.connect()
    sql="SELECT DATE_SUB(fecha,INTERVAL 30 MINUTE) fecha,CAST((Datos_Jenbacher_MGG02_I_iP+Datos_Jenbacher_MGG01_I_iP)/1300 AS UNSIGNED) MGGmotores from bd_parcbit.gemelo where date(fecha)='"+dia.strftime("%Y-%m-%d")+"' and MINUTE(fecha)=30"
    motores= pd.read_sql(sql, dbConnection)
    motores['fecha']=pd.to_datetime(motores['fecha'])
    #CARGA OMIE
    dialect='mysql+pymysql://powerBI:p0werb1@172.30.1.107:3306/sdi'
    sql="SELECT * from omiehorario WHERE date(fecha)='"+dia.strftime("%Y-%m-%d")+"';"
    sqlEngine=create_engine(dialect)
    dbConnection = sqlEngine.connect()
    omie= pd.read_sql(sql, dbConnection)
    #TRATAMIENTO
    if len(motores)>0 and len(omie)>0:
        df=pd.merge(motores, omie, on="fecha",how="left")
        resultado={'fecha':[df.loc[0,'fecha'].strftime("%Y-%m-%d")],
                '0mot_min':[df[df['MGGmotores']==0]['valor'].min()],'0mot_max':[df[df['MGGmotores']==0]['valor'].max()],'0mot_avg':[df[df['MGGmotores']==0]['valor'].mean()],
                '1mot_min':[df[df['MGGmotores']==1]['valor'].min()],'1mot_max':[df[df['MGGmotores']==1]['valor'].max()],'1mot_avg':[df[df['MGGmotores']==1]['valor'].mean()],
                '2mot_min':[df[df['MGGmotores']==2]['valor'].min()],'2mot_max':[df[df['MGGmotores']==2]['valor'].max()],'2mot_avg':[df[df['MGGmotores']==2]['valor'].mean()]}
        dfres=pd.DataFrame(data=resultado)
        dialect='mysql+pymysql://dams:G3M3L0@172.30.1.106:3306/aleasoft'
        sqlEngine=create_engine(dialect)
        dbConnection = sqlEngine.connect()
        dfres.to_sql(name='motoresparcbit', con=sqlEngine, if_exists='append')
    dia=dia+timedelta(days=1)
