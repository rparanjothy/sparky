
scala> val data=spark.read.format("csv").option("header","true").load("/app/data.csv")
data: org.apache.spark.sql.DataFrame = [Restaurant Name: string, Zip Code: string ... 5 more fields]

scala> data.printSchema
root
 |-- Restaurant Name: string (nullable = true)
 |-- Zip Code: string (nullable = true)
 |-- Inspection Date: string (nullable = true)
 |-- Score: string (nullable = true)
 |-- Address: string (nullable = true)
 |-- Facility ID: string (nullable = true)
 |-- Process Description: string (nullable = true)


scala>

scala> val d=spark.read.csv("/app/data.csv")
d: org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 5 more fields]

scala> d
res8: org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 5 more fields]

scala> d.printSchema
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)
 |-- _c5: string (nullable = true)
 |-- _c6: string (nullable = true)


scala>

val data=spark.read.format("csv").option("header","true").option("escape", "\"").option("inferSchema", "true").load("/app/data.csv")


// distinct a col
val zip = data.select('Zip)
zip.distinct

// sort
data.select('Zip).orderBy('Zip.desc).show(10)

// count distinct
val zip = data.select('Zip).distinct.count


// count restaurants by zip
data.select('Zip,'Name).groupBy('Zip).count.show
data.select('Zip,'Name).groupBy('Zip).count.orderBy('count.desc).show

// limit, returns a new object.
// show retuns none

// to load a file as a table

val df=spark.read.csv("/app/data.csv")
df.registerTempTable("fooddata")

// now you can saw

val ssql=spark.sqlContext.read.table("foodata")

//  f.rdd.partitions.size

// to materialize in the cache.

spark.sqlContext.cacheTable("foodata")

// now say
f.count // this will be faster


//  find count of words from names
val x=data.select('Name)
x.map(r=>r.getString(0)).flatMap(_.split(" ")).groupBy('value).count.orderBy('count.desc).show