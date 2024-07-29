import os
import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# --------------------------------------------------------------EXTRACT--------------------------------------------------------------
def Read_file_json(path):
    data = spark.read.format('json').option('header', 'true').load(path)
    data = data.select('_source.Contract', '_source.AppName', '_source.TotalDuration')
    return data

def Set_category(data):
    data = data.withColumn("Type",
       when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
      .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
                (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
      .when((col("AppName") == 'RELAX'), "Giải Trí")
      .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
      .when((col("AppName") == 'SPORT'), "Thể Thao")
      .otherwise("Error"))
    
    data = data.drop("AppName")
    return data

def Pivot_data(data):
    data = data.groupBy('Contract', 'Type').agg((sum('TotalDuration').alias('TotalDuration')))
    data = data.groupBy('Contract').pivot('Type').sum('TotalDuration')
    return data

def Extract_and_basic_Transform_1_day(directory_path, file_name):
    data = Read_file_json(os.path.join(directory_path, file_name))
    data = Set_category(data)
    data = Pivot_data(data)
    data = data.fillna(0)
    data = data.withColumn('Date', lit('-'.join([file_name[0:4], file_name[4:6], file_name[6:8]])))
    return data

def Extract_and_basic_Transform_30_day(directory_path):
    files = os.listdir(directory_path)
    files = [file for file in files if file != '.DS_Store']
    files.sort()

    data = 0
    for i in range(0, 30):
        if (data == 0):
            data = Extract_and_basic_Transform_1_day(directory_path, files[i])
        else:
            data = data.union(Extract_and_basic_Transform_1_day(directory_path, files[i]))

    return data

# --------------------------------------------------------------TRANSFORM--------------------------------------------------------------

def Get_activeness(data):
    right = data.select('Contract', 'Date').orderBy('Contract')
    right = right.groupBy('Contract').agg(countDistinct('Date').alias('Activeness'))
    data = data.groupBy('Contract').agg({'Giải Trí':'sum', 'Phim Truyện':'sum', 'Thiếu Nhi':'sum', 'Thể Thao':'sum', 'Truyền Hình':'sum'})
    data = data.withColumnsRenamed({'sum(Giải Trí)':'Giải Trí', 'sum(Phim Truyện)':'Phim Truyện', 'sum(Thiếu Nhi)':'Thiếu Nhi', 'sum(Thể Thao)':'Thể Thao', 'sum(Truyền Hình)':'Truyền Hình'})
    data = data.join(right,on='Contract',how='inner')
    return data

def Get_customer_taste(data):
    data=data.withColumn("Taste",concat_ws("-",
                                    when(col("Giải Trí") != 0,lit("Giải Trí"))
                                    ,when(col("Phim Truyện") != 0,lit("Phim Truyện"))
                                    ,when(col("Thể Thao") != 0,lit("Thể Thao"))
                                    ,when(col("Thiếu Nhi") != 0,lit("Thiếu Nhi"))
                                    ,when(col("Truyền Hình") != 0,lit("Truyền Hình"))))
    return data

def Get_most_watch(data):
    data=data.withColumn("MostWatch",greatest(col("Giải Trí"),col("Phim Truyện"),col("Thể Thao"),col("Thiếu Nhi"),col("Truyền Hình")))
    data=data.withColumn("MostWatch",
                    when(col("MostWatch")==col("Truyền Hình"),"Truyền Hình")
                    .when(col("MostWatch")==col("Phim Truyện"),"Phim Truyện")
                    .when(col("MostWatch")==col("Thể Thao"),"Thể Thao")
                    .when(col("MostWatch")==col("Thiếu Nhi"),"Thiếu Nhi")
                    .when(col("MostWatch")==col("Giải Trí"),"Giải Trí"))
    return data

def Transform_to_OLAP(data):
    data = Get_activeness(data)
    data = Get_most_watch(data)
    data = Get_customer_taste(data)
    return data

# --------------------------------------------------------------LOAD--------------------------------------------------------------

def Load(data):
    url = 'jdbc:mysql://localhost:3306/B4_ETL'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = '1'
    dbtable = 'table_name_1'
    data.write.format('jdbc').options(url = url , driver = driver , dbtable = dbtable , user=user , password = password).mode('overwrite').save()

# --------------------------------------------------------------EXEC--------------------------------------------------------------

def Main():
    directory_path = '/Users/quachtuankhoi/Downloads/BigData - GEN - 7/Log_Content'
    df = Extract_and_basic_Transform_30_day(directory_path=directory_path)
    df = Transform_to_OLAP(df)
    df = Load(df)

spark = SparkSession.builder.config("spark.jars.packages","com.mysql:mysql-connector-j:8.3.0").getOrCreate()
Main()
spark.stop()