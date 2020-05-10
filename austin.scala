
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


data.select('Name).filter('Name.contains("INDIA")).distinct.show(50,false)

data.select('Name).filter('Name.contains(name)).distinct.show(50,false)

scala> def byName(name:String)=data.select('Name).filter('Name.contains(name)).distinct.show(50,false)

// groupBy Zip
 data.select('Name,'Zip).groupBy('Zip.alias("zipCode")).agg(count('Name) as "S").show


// RDD 
// this is row; not optimized in Tungstun using sparkContext
val foodRDD = sc.textFile("/app/data.csv")
foodRDD.count
foodRDD.show(10,false)
foodRDD.take(2) // gives back a scala array of Strings!!

// DF
//  This is columnlar 

// DS
// This is also columnlar using sqlContext

val foodDS=spark.sqlContext.read.text("/app/data.csv")
foodDS.take(4) // gives back a scala array of ROWS!!
foodDS.take(4).foreach(println)

// Row methods
foodDS.take(4).foreach(x=> println(x.size))
// get each row as a string
// if there are more than one column in the row, then use the get(idx); 
//  to cast use getString, getInt..
foodDS.take(4).foreach(x=> println(x.getString(0)))



// ML

// just get one column DF
val names = data.select('Name)

// Define a tokenizer, set what should be the input, what should be the output.
//  we will then connect this tokenizer to a DF

val nameTokens=new RegexTokenizer().setInputCol("Name").setOutputCol("words").setPattern("\\W+")

// connect to a DF ; use transform
val wordsInName=nameTokens.transform(names)

scala> val wordsInName=nameTokens.transform(names)
wordsInName: org.apache.spark.sql.DataFrame = [Name: string, words: array<string>]

scala> wordsInName.show
+--------------------+--------------------+
|                Name|               words|
+--------------------+--------------------+
|SHADY GROVE PHARMACY|[shady, grove, ph...|
|           DON POLLO|        [don, pollo]|
|CALIFORNIA TORTIL...|[california, tort...|
|     FIVE GUYS #0095|  [five, guys, 0095]|
|   RANDOLPH PHARMACY|[randolph, pharmacy]|
|AVERY HOUSE FOR W...|[avery, house, fo...|
|GOLDEN HOUSE CARR...|[golden, house, c...|
|    MCDONALD'S #5715| [mcdonald, s, 5715]|
|MOMO CHICKEN & GRILL|[momo, chicken, g...|
|HAMPSHIRE GREENS ...|[hampshire, green...|
|B'NAI B'RITH HOME...|[b, nai, b, rith,...|
|ROCKVILLE GOURMET...|[rockville, gourm...|
|  CVS/PHARMACY #1879|[cvs, pharmacy, 1...|
|           SMOKE BBQ|        [smoke, bbq]|
|LEDO PIZZA - MONT...|[ledo, pizza, mon...|
|  SLIGO CREEK CENTER|[sligo, creek, ce...|
|       ST. ELMO DELI|    [st, elmo, deli]|
|    NYC PIZZA & SUBS|  [nyc, pizza, subs]|
|               JALEO|             [jaleo]|
| KELLY'S CAJUN GRILL|[kelly, s, cajun,...|
+--------------------+--------------------+

// Sampling.. use the sample method to get 10%
val wordsSampled= wordsInName.sample(false,.1,5) // here 5 is seed for randomness

// here wordsSapled will have 2 colums, the name and the words in the name.
//  take the words only
val wordsOnly=wordsSampled.select('words)

scala> val wordsOnly=wordsSampled.select('words)
wordsOnly: org.apache.spark.sql.DataFrame = [words: array<string>]

// this is array of array of words
//  flatmap using builtIn explode function.
scala> import org.apache.spark.sql.functions
import org.apache.spark.sql.functions

// select explode(value) from xxx

val allwords=wordsOnly.select(org.apache.spark.sql.functions.explode('words))

scala> allwords.printSchema
root
 |-- col: string (nullable = true)

 scala> allwords.show
+----------+
|       col|
+----------+
|     shady|
|     grove|
|  pharmacy|
|  mcdonald|
|         s|
|      5715|
|      momo|
|   chicken|
|     grill|
|       cvs|
|  pharmacy|
|      1879|
|    dunkin|
|    donuts|
|    330620|
|germantown|
|     lotte|
|    butler|
|         s|
|   orchard|
+----------+

// to give a custom name to the column, use as()
val allwordsWithColName=wordsOnly.select(org.apache.spark.sql.functions.explode('words).as("justWord"))
scala> allwordsWithColName.show
+----------+
|  justWord|
+----------+
|     shady|
|     grove|
|  pharmacy|
|  mcdonald|
|         s|
|      5715|
|      momo|
|   chicken|
|     grill|
|       cvs|
|  pharmacy|
|      1879|
|    dunkin|
|    donuts|
|    330620|
|germantown|
|     lotte|
|    butler|
|         s|
|   orchard|
+----------+