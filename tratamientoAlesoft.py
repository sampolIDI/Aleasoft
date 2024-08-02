import pandas as pd
import seaborn as sns
import os
from sqlalchemy import create_engine
import pymysql.cursors
import datetime

def addPrediccionCompleta(file,sqlEngine):
    #COMPLETO
    dfindicesDATE=pd.read_excel("./Aleasoft/"+file,usecols="B")
    dfindicesDATE=dfindicesDATE.loc[dfindicesDATE['Unnamed: 1']=="DATE"]
    dfindicesDATE.reset_index(inplace=True)
    #FOTOVOLTAICA
    df=pd.read_excel("./Aleasoft/"+file,header=dfindicesDATE.loc[10,'index']+1,nrows=240,usecols="B,C,E")
    df['fecha']=pd.to_datetime(df['DATE'])+pd.to_timedelta(df['HOUR']-1,unit="h")
    df.drop(columns=['DATE','HOUR'],inplace=True)
    df.rename({'FORE.': 'fotovoltaica'},axis='columns',inplace=True)
    #WIND
    df2=pd.read_excel("./Aleasoft/"+file,header=dfindicesDATE.loc[9,'index']+1,nrows=240,usecols="B,C,E")
    df2['fecha']=pd.to_datetime(df2['DATE'])+pd.to_timedelta(df2['HOUR']-1,unit="h")
    df2.drop(columns=['DATE','HOUR'],inplace=True)
    df2.rename({'FORE.': 'eolica'},axis='columns',inplace=True)
    df=pd.merge(df, df2, on="fecha",how="outer")
    #DEMAND
    df2=pd.read_excel("./Aleasoft/"+file,header=dfindicesDATE.loc[8,'index']+1,nrows=240,usecols="B,C,E")
    df2['fecha']=pd.to_datetime(df2['DATE'])+pd.to_timedelta(df2['HOUR']-1,unit="h")
    df2.drop(columns=['DATE','HOUR'],inplace=True)
    df2.rename({'FORE.': 'demanda'},axis='columns',inplace=True)
    df=pd.merge(df, df2, on="fecha",how="outer")
    #MATCHED DEMAND
    df2=pd.read_excel("./Aleasoft/"+file,header=dfindicesDATE.loc[7,'index']+1,nrows=240,usecols="B,C,E")
    df2['fecha']=pd.to_datetime(df2['DATE'])+pd.to_timedelta(df2['HOUR']-1,unit="h")
    df2.drop(columns=['DATE','HOUR'],inplace=True)
    df2.rename({'FORE.': 'matchdemand'},axis='columns',inplace=True)
    df=pd.merge(df, df2, on="fecha",how="outer")
    #PRICE
    df2=pd.read_excel("./Aleasoft/"+file,header=dfindicesDATE.loc[6,'index']+1,nrows=240,usecols="B,C,E")
    df2['fecha']=pd.to_datetime(df2['DATE'])+pd.to_timedelta(df2['HOUR']-1,unit="h")
    df2.drop(columns=['DATE','HOUR'],inplace=True)
    df2.rename({'FORE.': 'precio'},axis='columns',inplace=True)
    df=pd.merge(df, df2, on="fecha",how="outer")
    df['prediccion']=pd.to_datetime(file[0:14],format="%Y%m%d_%H_%M")
    df.set_index('fecha',inplace=True)
    df.to_sql(name='general', con=sqlEngine, if_exists='append')

def addPrediccionTemperatura(file,sqlEngine):
    dfindicesDATE=pd.read_excel("./Aleasoft/"+file,usecols="B")
    dfindicesDATE=dfindicesDATE.loc[dfindicesDATE['Unnamed: 1']=="DATE"]
    dfindicesDATE.reset_index(inplace=True)
    #TEMPERATURA
    df=pd.read_excel("./Aleasoft/"+file,header=dfindicesDATE.loc[5,'index']+1,nrows=21,usecols="B,D")
    df['fecha']=pd.to_datetime(df['DATE'])
    df['prediccion']=pd.to_datetime(file[0:14],format="%Y%m%d_%H_%M")
    df.drop(columns=['DATE'],inplace=True)
    df.dropna(inplace=True)
    df.set_index('fecha',inplace=True)
    df.rename({'TMED FORE.': 'temperatura'},axis='columns',inplace=True)
    df.to_sql(name='temperatura', con=sqlEngine, if_exists='append')

def addAnalisis(file,sqlEngine):
    dfindicesDATE=pd.read_excel("./Aleasoft/"+file,usecols="B")
    dfindicesDATE=dfindicesDATE.loc[dfindicesDATE['Unnamed: 1']=="DATE"]
    dfindicesDATE.reset_index(inplace=True)
    df=pd.read_excel("./Aleasoft/"+file,header=dfindicesDATE.loc[0,'index']+1,nrows=24,usecols="B,C,D,E")
    df['fecha']=pd.to_datetime(df['DATE'])+pd.to_timedelta(df['HOUR']-1,unit="h")
    df.drop(columns=['DATE','HOUR'],inplace=True)
    df.rename({'FORE.': 'precioforecast','MARKET PRICE':'precioreal'},axis='columns',inplace=True)
    df2=pd.read_excel("./Aleasoft/"+file,header=dfindicesDATE.loc[3,'index']+1,nrows=24,usecols="B,C,D,E")
    df2['fecha']=pd.to_datetime(df2['DATE'])+pd.to_timedelta(df2['HOUR']-1,unit="h")
    df2.drop(columns=['DATE','HOUR'],inplace=True)
    df2.rename({'FORE.': 'matchdemandforecast','MATCHED DEMAND':'matchdemandreal'},axis='columns',inplace=True)
    df=pd.merge(df, df2, on="fecha",how="outer")
    df['prediccion']=pd.to_datetime(file[0:14],format="%Y%m%d_%H_%M")
    df.set_index('fecha',inplace=True)
    df.to_sql(name='analisis', con=sqlEngine, if_exists='append')

def compruebaArchivo(file,tabla,sqlEngine):
    fecha=datetime.datetime.strptime(file[0:14],"%Y%m%d_%H_%M").strftime("%Y-%m-%d %H:%M:%S")
    sql="select count(*) val from "+tabla+" WHERE prediccion='"+fecha+"';"
    dbConnection = sqlEngine.connect()
    df=pd.read_sql(sql,dbConnection)
    if df.values>0:
        return True
    else:
        return False

def ls2(path): 
    return [obj.name for obj in os.scandir(path) if obj.is_file()]
files=ls2("./Aleasoft")
dialect='mysql+pymysql://dams:G3M3L0@172.30.1.106:3306/aleasoft'
sqlEngine=create_engine(dialect)
for index,file in enumerate(files):
    print(file)
    if file[-16:]=="SDM_Analysis.xls":
        if (compruebaArchivo(file,"general",sqlEngine)==False):
            addPrediccionCompleta(file,sqlEngine)
            addPrediccionTemperatura(file,sqlEngine)
            os.remove("./Aleasoft/"+file)
            print(">> cargado en DB y eliminado")
        else:
            os.remove("./Aleasoft/"+file)
            print(">> ya existía, eliminado")
    elif file[-24:]=="SDM_D2toD11_Analysis.xls":
        if (compruebaArchivo(file,"general",sqlEngine)==False):
            addPrediccionCompleta(file,sqlEngine)
            os.remove("./Aleasoft/"+file)
            print(">> cargado en DB y eliminado")
        else:
            os.remove("./Aleasoft/"+file)
            print(">> ya existía, eliminado")
    elif file[-22:]=="SDM_Analysis_REALS.xls":
        if (compruebaArchivo(file,"analisis",sqlEngine)==False):
            addAnalisis(file,sqlEngine)
            os.remove("./Aleasoft/"+file)
            print(">> cargado en DB y eliminado")
        else:
            os.remove("./Aleasoft/"+file)
            print(">> ya existía, eliminado")            
    else:
        print(">> archivo desconocido")