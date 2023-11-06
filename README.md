# Mini Project 10: Pyspark Data Processing
#### Katelyn Hucker (kh509)
The requirements of this project were: 
  - Use PySpark to perform data processing on a large dataset
  - Include at least one Spark SQL query and one data transformation

### PySpark Processing on a large dataset: 

I took a Dungeons and Dragons dataset which contains information about different characters that can be created. I initialize the dataset using pyspark with the following 2 functions.

```plaintext
def initialize_spark():
    """
    Initialize a SparkSession and return it.
    """
    spark = SparkSession.builder.appName("Week 10 Assignment").getOrCreate()
    return spark


def read_tsv(spark, file_path):
    df = spark.read.option("header", "true").option("delimiter", "\t").csv(file_path)
    return df
```

### SQL Query and Data Transformation

I did a SQL Query on the dataframe to display the alignment column of the D&D data. It does a Spark SQL Query by only getting the 'CG' string data of the alignment column.  I then perform a data transformation by only keeping characters which have level equal to 1. This query and transformation process can be shown in the below snapshot from codespaces. 
![image](https://github.com/nogibjj/kh509_miniproject10/assets/143521756/b604a9a6-0ff6-46b0-a635-9e8052ccb047)



The data transformation results are then saved in results.txt. Here is the screen capture of the results. 

![image](https://github.com/nogibjj/kh509_miniproject10/assets/143521756/c324fd38-fb7c-4f75-8eee-fc9405fc37f4)

