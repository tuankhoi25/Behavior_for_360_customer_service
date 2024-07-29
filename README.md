# Behavior_for_360_customer_service
The objective of this project is to conduct a behavioral analysis of users interacting with the service throughout April 2022.

## About the data 
User interaction data is stored in JSON format for further analysis.

## Procedure 
The data is processed using PySpark and then stored in a MySQL database.

### Extract data
During this process, we will read the data, perform basic statistics for each day of the month, and then union the results to understand how much time users spend watching different genres each day.
```python
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
```

#### The specific function of each method

##### Extract_and_basic_Transform_30_day function
This code takes a directory path as a parameter, which contains 30 days of data files. Each file will be read and processed by the Extract_and_basic_Transform_1_day function and then unioned together in ascending order by date. The final output of the Extract_and_basic_Transform_30_day function is a DataFrame containing the data from all 30 days.
```python
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
```

##### Extract_and_basic_Transform_1_day function
After being called in the Extract_and_basic_Transform_30_day function, this function will read the specific file's data and perform some basic data transformations such as:
* Reading data from the file.
* Mapping specific categories to each channel.
* Creating a dataframe that shows the total viewing hours for each genre of movie for each user.
* Cleaning the data by replacing NULL values with 0.
* Adding a 'Date' column to distinguish the data of each day.
```python
def Extract_and_basic_Transform_1_day(directory_path, file_name):
    data = Read_file_json(os.path.join(directory_path, file_name))
    data = Set_category(data)
    data = Pivot_data(data)
    data = data.fillna(0)
    data = data.withColumn('Date', lit('-'.join([file_name[0:4], file_name[4:6], file_name[6:8]])))
    return data
```

##### Read_file_json function
Retrieve the total time each user spends watching movies.
```python
def Read_file_json(path):
    data = spark.read.format('json').option('header', 'true').load(path)
    data = data.select('_source.Contract', '_source.AppName', '_source.TotalDuration')
    return data
```

##### Set_category function
Add a 'Type' column to store the movie genre corresponding to each channel in the 'AppName' column.
```python
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
```

##### Set_category function
Calculate the total viewing hours for each movie genre for each user.
```python
def Pivot_data(data):
    data = data.groupBy('Contract', 'Type').agg((sum('TotalDuration').alias('TotalDuration')))
    data = data.groupBy('Contract').pivot('Type').sum('TotalDuration')
    return data
```

### Transform data
In this process, we will transform the data obtained from the Extract data phase to determine how many days each user watched movies in total during April 2022, which movie genres were included, and which genre was watched the most.
```python
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
    data = Get_customer_taste(data)
    data = Get_most_watch(data)
    return data
```

#### The specific function of each method.

##### Get_activeness function
Determine the number of active days for each user by counting the unique values in the 'Date' column, while also calculating the total hours each user spent over the 30 days watching each movie genre.
```python
def Get_activeness(data):
    right = data.select('Contract', 'Date').orderBy('Contract')
    right = right.groupBy('Contract').agg(countDistinct('Date').alias('Activeness'))
    data = data.groupBy('Contract').agg({'Giải Trí':'sum', 'Phim Truyện':'sum', 'Thiếu Nhi':'sum', 'Thể Thao':'sum', 'Truyền Hình':'sum'})
    data = data.withColumnsRenamed({'sum(Giải Trí)':'Giải Trí', 'sum(Phim Truyện)':'Phim Truyện', 'sum(Thiếu Nhi)':'Thiếu Nhi', 'sum(Thể Thao)':'Thể Thao', 'sum(Truyền Hình)':'Truyền Hình'})
    data = data.join(right,on='Contract',how='inner')
    return data
```

##### Get_customer_taste function
Aggregate the movie genres that users watch based on the number of hours they spend watching each genre.
```python
def Get_customer_taste(data):
    data=data.withColumn("Taste",concat_ws("-",
                                    when(col("Giải Trí") != 0,lit("Giải Trí"))
                                    ,when(col("Phim Truyện") != 0,lit("Phim Truyện"))
                                    ,when(col("Thể Thao") != 0,lit("Thể Thao"))
                                    ,when(col("Thiếu Nhi") != 0,lit("Thiếu Nhi"))
                                    ,when(col("Truyền Hình") != 0,lit("Truyền Hình"))))
    return data
```

##### Get_most_watch function
Identify the movie genre that users spend the most time watching.
```python
def Get_most_watch(data):
    data=data.withColumn("MostWatch",greatest(col("Giải Trí"),col("Phim Truyện"),col("Thể Thao"),col("Thiếu Nhi"),col("Truyền Hình")))
    data=data.withColumn("MostWatch",
                    when(col("MostWatch")==col("Truyền Hình"),"Truyền Hình")
                    .when(col("MostWatch")==col("Phim Truyện"),"Phim Truyện")
                    .when(col("MostWatch")==col("Thể Thao"),"Thể Thao")
                    .when(col("MostWatch")==col("Thiếu Nhi"),"Thiếu Nhi")
                    .when(col("MostWatch")==col("Giải Trí"),"Giải Trí"))
    return data
```

### Load data
Store the OLAP output just obtained into the MySQL database.
```python
def Load(data):
    url = 'jdbc:mysql://localhost:3306/B4_ETL'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = '1'
    dbtable = 'table_name_1'
    data.write.format('jdbc').options(url = url , driver = driver , dbtable = dbtable , user=user , password = password).mode('overwrite').save()
```

### Final data
|      Contract|Thể Thao|Phim Truyện|Thiếu Nhi|Giải Trí|Truyền Hình|Activeness|  MostWatch|               Taste|
|--------------|--------|-----------|---------|--------|-----------|----------|-----------|--------------------|
|             0|       0|       6127|        0|       0| 1483915581|         3|Truyền Hình|Phim Truyện-Truyề...|
|113.182.209.48|       0|          0|        0|      89|         63|         1|   Giải Trí|Giải Trí-Truyền Hình|
|     AGAAA0335|       0|          0|        0|       0|      27506|         3|Truyền Hình|         Truyền Hình|
|     AGAAA0338|       0|          0|        0|       0|      26104|         3|Truyền Hình|         Truyền Hình|
|     AGAAA0342|       0|          0|        0|       0|       8849|         2|Truyền Hình|         Truyền Hình|
|     AGAAA0345|       0|          0|        0|       0|      14276|         3|Truyền Hình|         Truyền Hình|
|     AGAAA0346|       0|          0|        0|       0|     182602|         3|Truyền Hình|         Truyền Hình|
|     AGAAA0350|       0|          0|        0|       0|      27227|         3|Truyền Hình|         Truyền Hình|
|     AGAAA0352|       0|          0|        0|       0|      39196|         3|Truyền Hình|         Truyền Hình|
|     AGAAA0353|       0|       1665|        0|       0|       1316|         3|Phim Truyện|Phim Truyện-Truyề...|
|     AGAAA0354|       0|          0|        0|       0|       1229|         1|Truyền Hình|         Truyền Hình|
|     AGAAA0356|       0|          0|        0|       0|      90248|         3|Truyền Hình|         Truyền Hình|
|     AGAAA0357|       0|          0|        0|       0|       2007|         3|Truyền Hình|         Truyền Hình|
|     AGAAA0359|       0|          0|        0|       0|     245481|         3|Truyền Hình|         Truyền Hình|
|     AGAAA0360|       0|       2079|        0|       0|      14936|         3|Truyền Hình|Phim Truyện-Truyề...|
|     AGAAA0366|       0|          0|        0|       0|     137085|         3|Truyền Hình|         Truyền Hình|
|     AGAAA0370|       0|          0|        0|       0|     255959|         3|Truyền Hình|         Truyền Hình|
|     AGAAA0372|       0|          0|        0|       0|         78|         2|Truyền Hình|         Truyền Hình|
|     AGAAA0375|       0|        114|        0|       0|      59377|         3|Truyền Hình|Phim Truyện-Truyề...|
|     AGAAA0376|       0|          0|        0|       0|      18492|         3|Truyền Hình|         Truyền Hình|

### Results Achieved from the Data ETL Process

#### Activeness
The data obtained from the Activeness column helps identify which customers are loyal and which are at risk of discontinuing the service, allowing for the development of corresponding strategies.

For example: Implement a loyalty program (such as discount programs, point accumulation, birthday gifts, etc.) and personalize the experience for loyal customers to make them feel valued and more engaged with the service. Additionally, for customers at risk of discontinuing the service, special offers can be applied to retain them, and in-depth surveys and analysis of their behavior can be conducted to identify reasons for their potential departure.

#### Taste
The data obtained from the Taste column helps identify customers' favorite movie genres, allowing for the development of corresponding strategies.

For example: Personalize user experiences by suggesting movies based on their preferences. This can be done by analyzing the genres of movies they have watched previously and recommending films of similar genres.

#### MostWatch
The data obtained from the MostWatch column helps us determine the movie genres that customers prefer the most, allowing for the development of specific strategies.

For example: Instead of targeting all customers to introduce them to football packages (K+, FPT play, etc.), football viewing devices (TVs, speakers, etc.), or football-related accessories (shirts, shorts, posters, etc. featuring favorite players or teams), we can directly target those who most enjoy watching football to save costs.