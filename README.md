# Spark-Using-Python-

--> Here i have Created a Spark Session and worked on the window aggregations for some practise.

--> When it comes to aggreations spark contains three types of aggreations.

1. Window Aggregations
2. Grouping Aggregations
3. Simple Aggregations.

Here i am only working on Window aggregations. 

--> Window aggregation is nothing but here we are dealing with a fixed size window. 
--> It's like a basic function like create a function and call it when required.
--> You can work on rank, dense_rank, row_number fucntions in spark using this window fucntion.

--> window function basically takes two main important things one is partition by and other one is order. i have showed an example in the notebook file.

# Creating_User_defined_Fucntions_Spark

--> Please Use Dataset1 as a loadable file.

So, here i am working on UDF basically in Spark:

--> In Spark there are two different kind of UDF's.

1. UDF with Structured API's
2. UDF with SQl/String Expression

--> Everytime we create a UDF we need to register it to use that. Register means giving an authorization to the function and telling spark to allow our function.

Register USing first Kind:

spark.udf.register("name_you_want_to_register", fucntion_name, Type)
**Example:**
spark.udf.register("parseAgeCheck", ageCheck, StringType())

Another Type:
name_ypu_want = udf(function_name, Type)
**Example:**
parseAgeCheck2 = udf(ageCheck2,StringType())

# For more reference of UDF:
https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/

# Simple_Aggreagtions_GroupBy_Aggregations

Dataset Used: Orders_Data_Reduced.xsls

So, Here i have disscussed about some of the basic sample aggreagtions and GroupBy aggregations and showed an example of each.

In my view what exactly is simple aggregations. You know it is like when you want calcualte of something total like a sum of sales, avg of something like that..

and coming to groupBy as we all know what does it do that what it does haha...

--> I have done the same problem in three different ways like column string and column object and spark sql.. As you can see it in the python notebook i attached which is called "Simple_Aggreagtions_GroupBy_Aggregations"

--> One more thing remember is if you observe there is some difference in terms of writing code using simple aggregation and groupBy it is like i have used select transformation in simple agg and caluclting what i want but in groupBy i haven't used the select keyword to select columns but instead i have done using "agg" it is because groupBy is a transformation and it is not providing the select transformation.

--> you can ask it is not possible to use select with groupBy yes it is possible when you use spark sql if you dont want to use spark sql then you dont have option to use but you can use like i used.

Example: you can also use like these. 

from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("groupBy and select example") \
        .getOrCreate()

df = spark.read.csv("path/to/data.csv", header=True, inferSchema=True)

grouped_df = df.groupBy("column1", "column2")
agg_df = grouped_df.agg({"column3": "sum", "column4": "avg"})
selected_df = agg_df.select("column1", "column2", "sum(column3)", "avg(column4)")

selected_df.show()



