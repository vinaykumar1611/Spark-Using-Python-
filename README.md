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


