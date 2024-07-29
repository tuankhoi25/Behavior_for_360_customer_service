# Behavior_for_360_customer_service
Dự án này được thiết kế nhằm phân tích hành vi sử dụng dịch vụ của người dùng trong 30 ngày tháng 04/2022.

## About the data 
Thời gian sử dụng dịch vụ của người dùng được lưu trữ theo định dạng JSON.

## Procedure 
Dữ liệu được xử lý bằng Pyspark và sau đó được lưu trữ vào cơ sở dữ liệu MySQL.

### Extract data
Trong quá trình này, chúng ta sẽ đọc và tiến hành thống kê cơ bản dữ liệu của từng ngày trong tháng và union lại để nó phù hợp để biết được mỗi ngày người dùng sẽ bỏ ra bao nhiêu thời gian để xem những thể loại phim khác nhau.
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
    files.sort()
    print(files)

    data = 0
    for i in range(0, 30):
        if (data == 0):
            data = Extract_and_basic_Transform_1_day(directory_path, files[i])
        else:
            data = data.union(Extract_and_basic_Transform_1_day(directory_path, files[i]))

    return data
```

#### Chức năng cụ thể của từng hàm

##### Hàm Extract_and_basic_Transform_30_day
Đoạn mã này có một tham số là đường dẫn của thư mục chứa dữ liệu của 30 ngày. Những file dữ liệu này sẽ được đọc và xử lý thông qua hàm Extract_and_basic_Transform_1_day và union lại với nhau theo ngày tăng dần. Kết quả trả về của hàm Extract_and_basic_Transform_30_day chính là một dataframe chứa dữ liệu của 30 ngày. 
```python
def Extract_and_basic_Transform_30_day(directory_path):
    files = os.listdir(directory_path)
    files.sort()
    print(files)

    data = 0
    for i in range(0, 30):
        if (data == 0):
            data = Extract_and_basic_Transform_1_day(directory_path, files[i])
        else:
            data = data.union(Extract_and_basic_Transform_1_day(directory_path, files[i]))

    return data
```

##### Hàm Extract_and_basic_Transform_1_day
Sau khi được gọi trong hàm Extract_and_basic_Transform_30_day, hàm này sẽ tiến hành đọc dữ liệu của file cụ thể và tiến hành một số bước transform dữ liệu cơ bản như: 
* Đọc dữ liệu từ file.
* Ánh xạ các danh mục cụ thể cho từng kênh.
* Tạo ra dataframe cho biết tổng số giờ xem cho từng thể loại phim của từng người dùng.
* Làm sạch dữ liệu bằng cách thay các giá trị NULL bằng giá trị 0
* Thêm cột 'Date' để phân biệt dữ liệu của từng ngày.
```python
def Extract_and_basic_Transform_1_day(directory_path, file_name):
    data = Read_file_json(os.path.join(directory_path, file_name))
    data = Set_category(data)
    data = Pivot_data(data)
    data = data.fillna(0)
    data = data.withColumn('Date', lit('-'.join([file_name[0:4], file_name[4:6], file_name[6:8]])))
    return data
```

##### Hàm Read_file_json
Lấy dữ liệu về tổng thời gian mà từng người dùng bỏ ra để xem phim.
```python
def Read_file_json(path):
    data = spark.read.format('json').option('header', 'true').load(path)
    data = data.select('_source.Contract', '_source.AppName', '_source.TotalDuration')
    return data
```

##### Hàm Set_category
Thêm cột "Type" để lưu trữ thể loại phim tương ứng với từng kênh của cột "AppName".
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

##### Hàm Set_category
Tính tổng số giờ xem cho từng thể loại phim của từng người dùng
```python
def Pivot_data(data):
    data = data.groupBy('Contract', 'Type').agg((sum('TotalDuration').alias('TotalDuration')))
    data = data.groupBy('Contract').pivot('Type').sum('TotalDuration')
    return data
```

### Transform data
Trong quá trình này, chúng ta sẽ biến đổi những dữ liệu thu được từ quá trình Extract data để tìm xem trong cả tháng 4/2022 mỗi người dùng đã xem phim tổng cộng bao nhiêu ngày, bao gồm những thể loại phim nào và thể loại phim nào là được xem nhiều nhất.
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

#### Chức năng cụ thể của từng hàm

##### Hàm Get_activeness
Tìm ra số ngày hoạt động của từng người dùng bằng cách đếm những giá trị duy nhất ở cột "Date", đồng thời tính tổng số giờ mà người dùng đã bỏ ra trong 30 ngày để xem từng thể loại phim.
```python
def Get_activeness(data):
    right = data.select('Contract', 'Date').orderBy('Contract')
    right = right.groupBy('Contract').agg(countDistinct('Date').alias('Activeness'))
    data = data.groupBy('Contract').agg({'Giải Trí':'sum', 'Phim Truyện':'sum', 'Thiếu Nhi':'sum', 'Thể Thao':'sum', 'Truyền Hình':'sum'})
    data = data.withColumnsRenamed({'sum(Giải Trí)':'Giải Trí', 'sum(Phim Truyện)':'Phim Truyện', 'sum(Thiếu Nhi)':'Thiếu Nhi', 'sum(Thể Thao)':'Thể Thao', 'sum(Truyền Hình)':'Truyền Hình'})
    data = data.join(right,on='Contract',how='inner')
    return data
```

##### Hàm Get_customer_taste
Tổng hợp những thể loại phim mà người dùng xem dựa trên số giờ mà họ bỏ ra để xem từng thể loại phim đó.
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

##### Hàm Get_most_watch
Tìm ra thể loại phim mà người dùng dành ra nhiều thời gian nhất để xem.
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
Lưu trữ OLAP output vừa tìm được vào cơ sở dữ liệu MYSQL
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

### Basic insight