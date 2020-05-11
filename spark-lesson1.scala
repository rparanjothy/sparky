Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.5
      /_/

Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_252)
Type in expressions to have them evaluated.
Type :help for more information.

scala> ^R


scala> ^R


scala> val ids = spark.range(1000)
ids: org.apache.spark.sql.Dataset[Long] = [id: bigint]

scala> ids.printSchema
root
 |-- id: long (nullable = false)


scala> ids.sum("id")
<console>:26: error: value sum is not a member of org.apache.spark.sql.Dataset[Long]
       ids.sum("id")
           ^

scala> ids.agg(sum('id))
res2: org.apache.spark.sql.DataFrame = [sum(id): bigint]

scala> .show
+-------+
|sum(id)|
+-------+
| 499500|
+-------+


scala> ids.groupBy('id%2)
res4: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [('id % 2)], value: [id: bigint], type: GroupBy]

scala> .show
<console>:26: error: value show is not a member of org.apache.spark.sql.RelationalGroupedDataset
       res4.show
            ^

scala> res4.min
<console>:26: error: missing argument list for method min in class RelationalGroupedDataset
Unapplied methods are only converted to functions when a function type is expected.
You can make this conversion explicit by writing `min _` or `min(_)` instead of `min`.
       res4.min
            ^

scala> res4.min()
res7: org.apache.spark.sql.DataFrame = [(id % 2): bigint, min(id): bigint]

scala> .show
+--------+-------+
|(id % 2)|min(id)|
+--------+-------+
|       0|      0|
|       1|      1|
+--------+-------+


scala> ids.groupBy('id %5).min.show
<console>:26: error: missing argument list for method min in class RelationalGroupedDataset
Unapplied methods are only converted to functions when a function type is expected.
You can make this conversion explicit by writing `min _` or `min(_)` instead of `min`.
       ids.groupBy('id %5).min.show
                           ^

scala> ids.groupBy('id % 5).min().show
+--------+-------+
|(id % 5)|min(id)|
+--------+-------+
|       0|      0|
|       1|      1|
|       3|      3|
|       2|      2|
|       4|      4|
+--------+-------+


scala> ids.groupBy('id % 5).max().show
+--------+-------+
|(id % 5)|max(id)|
+--------+-------+
|       0|    995|
|       1|    996|
|       3|    998|
|       2|    997|
|       4|    999|
+--------+-------+


scala> ids.groupBy('id %5).avg().show
+--------+-------+
|(id % 5)|avg(id)|
+--------+-------+
|       0|  497.5|
|       1|  498.5|
|       3|  500.5|
|       2|  499.5|
|       4|  501.5|
+--------+-------+


scala> ids.groupBy('id % 5).count().show
+--------+-----+
|(id % 5)|count|
+--------+-----+
|       0|  200|
|       1|  200|
|       3|  200|
|       2|  200|
|       4|  200|
+--------+-----+


scala> val grpBy='id % 5
grpBy: org.apache.spark.sql.Column = (id % 5)

scala> ids.groupBy(grpBy)
res14: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [('id % 5)], value: [id: bigint], type: GroupBy]

scala> ids.groupBy(grpBy as "grp").sum()
res15: org.apache.spark.sql.DataFrame = [grp: bigint, sum(id): bigint]

scala> res15.show
+---+-------+
|grp|sum(id)|
+---+-------+
|  0|  99500|
|  1|  99700|
|  3| 100100|
|  2|  99900|
|  4| 100300|
+---+-------+


scala> ids.groupBy(grpBy as "grp").sum('id as "sum").show
<console>:28: error: type mismatch;
 found   : org.apache.spark.sql.Column
 required: String
       ids.groupBy(grpBy as "grp").sum('id as "sum").show
                                           ^

scala> ids.groupBy(grpBy as "grp").sum("id").show()
+---+-------+
|grp|sum(id)|
+---+-------+
|  0|  99500|
|  1|  99700|
|  3| 100100|
|  2|  99900|
|  4| 100300|
+---+-------+


scala> ids.groupBy(grpBy as "grp").sum("id" as "S").show()
<console>:28: error: value as is not a member of String
       ids.groupBy(grpBy as "grp").sum("id" as "S").show()
                                            ^

scala> ids.groupBy(grpBy as "grp").agg(sum() as "s").show()
<console>:28: error: overloaded method value sum with alternatives:
  (columnName: String)org.apache.spark.sql.Column <and>
  (e: org.apache.spark.sql.Column)org.apache.spark.sql.Column
 cannot be applied to ()
       ids.groupBy(grpBy as "grp").agg(sum() as "s").show()
                                       ^

scala> ids.groupBy(grpBy as "grp").agg(sum('id) as "S")
res21: org.apache.spark.sql.DataFrame = [grp: bigint, S: bigint]

scala> .show
+---+------+
|grp|     S|
+---+------+
|  0| 99500|
|  1| 99700|
|  3|100100|
|  2| 99900|
|  4|100300|
+---+------+


scala>
scala> ids.join(sumByGrp).where('id%5 === 'grp).show
+---+---+-----+
| id|grp|    S|
+---+---+-----+
|995|  0|99500|
|990|  0|99500|
|985|  0|99500|
|980|  0|99500|
|975|  0|99500|
|970|  0|99500|
|965|  0|99500|
|960|  0|99500|
|955|  0|99500|
|950|  0|99500|
|945|  0|99500|
|940|  0|99500|
|935|  0|99500|
|930|  0|99500|
|925|  0|99500|
|920|  0|99500|
|915|  0|99500|
|910|  0|99500|
|905|  0|99500|
|900|  0|99500|
+---+---+-----+
only showing top 20 rows

projections:
scala> sumByGrp.join(ids).drop('grp).where('id <= 'S).show
+-----+---+
|    S| id|
+-----+---+
|99500|  0|
|99500|  1|
|99500|  2|
|99500|  3|
|99500|  4|
|99500|  5|
|99500|  6|
|99500|  7|
|99500|  8|
|99500|  9|
|99500| 10|
|99500| 11|
|99500| 12|
|99500| 13|
|99500| 14|
|99500| 15|
|99500| 16|
|99500| 17|
|99500| 18|
|99500| 19|
+-----+---+
only showing top 20 rows


scala> ids.groupBy(grpBy).agg(collect_list('id)).show
+--------+--------------------+
|(id % 5)|    collect_list(id)|
+--------+--------------------+
|       0|[0, 5, 10, 15, 20...|
|       1|[1, 6, 11, 16, 21...|
|       3|[3, 8, 13, 18, 23...|
|       2|[2, 7, 12, 17, 22...|
|       4|[4, 9, 14, 19, 24...|
+--------+--------------------+


scala> ids.orderBy('id)
res49: org.apache.spark.sql.Dataset[Long] = [id: bigint]

scala> ids.orderBy('id).show
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
|  7|
|  8|
|  9|
| 10|
| 11|
| 12|
| 13|
| 14|
| 15|
| 16|
| 17|
| 18|
| 19|
+---+
only showing top 20 rows


scala> ids.orderBy('id.desc).show
+---+
| id|
+---+
|999|
|998|
|997|
|996|
|995|
|994|
|993|
|992|
|991|
|990|
|989|
|988|
|987|
|986|
|985|
|984|
|983|
|982|
|981|
|980|
+---+
only showing top 20 rows


scala>