## Spark编程与源代码分析

作者： 刘远见

[Spark编程与源代码分析](#spark编程与源代码分析)

 [一、Spark核心概念](#一spark核心概念)

 [二、Spark的Minimum Useable Application](#二spark的minimum-useable-application)

 [三、Spark 2.2的DataFrame和DataSet (小小对比Java和Scala)](#三spark-22的dataframe和dataset-小小对比java和scala)

[3.1 DataFrame](#31-dataframe)auto

 [3.2 DataSet](#32-dataset)auto 

[3.3 通过DataFrame实现电影点评系统](#33-通过dataframe实现电影点评系统)

 [3.4 用Java 实现的一个“简单”的程序（二次排序）](#34-用java-实现的一个简单的程序二次排序)

[3.5 用Scala实现刚刚Java实现的二次排序，做相同的事](#35-用scala实现刚刚java实现的二次排序做相同的事)

[四、Spark 2.2 的流处理 Structured Streaming](#四spark-22-的流处理-structured-streaming)auto 

[4.1 一个WordCount 程序](#41-一个wordcount-程序)

[4.2 对实时数据的支持](#42-对实时数据的支持)

### 一、Spark核心概念

1. Master: 就像Hadoop拥有NameNode和DataNode一样，Spark有Master和Worker。Master是集群的领导者，负责管理集群资源，接受Client提交的作业，以及向Worker发送命令。
2. Driver: 一个Spark作业运行时会启动一个Driver进程，也是作业的主进程，负责作业的解析、生成Stage，并调度Task到Executor上。
3. Executor: 真正执行作业的地方。Executor分布在集群中的Worker上，每个Executor接收Driver的命令来加载和运行Task，一个Executor可以执行一个到多个Task.
4. SparkContext: 是程序运行调度的核心，由高层调度器DAGScheduler划分程序的每个阶段，底层调度器TaskScheduler划分每个阶段的具体任务。SchedulerBackend管理整个集群中为正在运行的程序分配计算资源Executor.
5. Task: 任务执行的工作单位，每个Task会被发送到一个节点上，每个Task对应RDD的一个partition.
6. DAGScheduler: 负责高层调度，划分stage并生成程序运行的有向无环图。
7. TaskScheduler: 负责具体stage内部的底层调度，具体task的调度、容错等。
8. Job: （正在执行的是ActiveJob）是Top-level的工作单位，每个Action算子都会触发一次Job，一个Job可能包含一个或多个Stage
9. Stage: 用来计算中间结果的Tasksets. Tasksets中的Task逻辑对于同一个RDD内的不同partition都一样。Stage在Shuffle的地方产生，此时下一个Stage要用到上一个Stage的全部数据，所以要等到上一个Stage全部执行完才能开始。Stage有两种：ShuffleMapStage和ResultStage.除了最后一个Stage是ResultStage，其他stage都是ShuffleMapStage。ShuffleMapStage会产生中间结果，以文件的方式保存在集群里，Stage经常被不同的Job共享，前提是这些Job重用了同一个RDD。
10. RDD: 是不可变的、Lazy级别的、粗粒度的（数据集级别的而不是单个数据级别的）数据集合，包含了一个或多个数据分片，即partition

### 二、Spark的Minimum Useable Application

Spark是一个非常复杂的东西，在没有弄明白Spark到底是什么以前，我觉得先用实例来说明上面的概念，并且构建一个可以允许的程序来看看Spark对数据到底能做什么处理，以及如何进行编程。首先我给出一个Scala的例子。Scala是一门类Java语言，它结合了面向对象编程和函数式编程，它是纯面向对象的。Scala被设计成可以和Java和C#无缝对接。Scala可以调用Java发放，创建Java对象，继承Java类和实现Java接口。这些都不需要额外的接口定义和胶合代码。

我们接下来处理一个电影点评系统，包括三个数据文件 users.dat, ratings.dat, movies.dat

其中users.dat的格式如下：

> UserID::Gender::Age::Occupation::Zip-code
>
> ```
> 1::F::1::10::48067
> 2::M::56::16::70072
> ```

这个文件有6040个用户的信息，然后ratings.dat的格式如下：

> UserID::MovieID::Rating::Timestamp
>
> ```
> 1::914::3::978301968
> 1::3408::4::978300275
> 1::2355::5::978824291
> 1::1197::3::978302268
> 1::1287::5::978302039
> ```

这个文件记录的是评分信息，然后是movies.dat，格式如下

> MovieID::Title::Genres
>
> ```
> 1::Toy Story (1995)::Animation|Children's|Comedy
> 2::Jumanji (1995)::Adventure|Children's|Fantasy
> 3::Grumpier Old Men (1995)::Comedy|Romance
> ```

还有occupations.dat，格式如下

> OccupationID:OccupationName
>
> ```
> 4::college/grad student
> 5::customer service
> 6::doctor/health care
> 7::executive/managerial
> ```

第一段代码，这部分对数据文件进行读取，存入RDD,我们把master节点设置为本机

```scala
    Logger.getLogger("org").setLevel(Level.ERROR)

    var masterUrl = "local[4]" //默认程序运行在本地Local模式中,测试不需要docker或服务器
    var dataPath = "data/moviedata/medium/"  //数据存放的目录；

	/**
      * 创建Spark集群上下文sc，在sc中可以进行各种依赖和参数的设置等
      */
    val sc = new SparkContext(
        new SparkConf().setMaster(masterUrl).
        setAppName("Movie_Users_Analyzer_RDD"))

    /**
      * 读取数据，用什么方式读取数据呢？在这里是使用RDD!
      */
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
//    val ratingsRDD = sc.textFile("data/moviedata/large/" + "ratings.dat")
	spark.stop
```

我们在这里做一个最简单的案例计算，给出一个完整可运行的程序，这个案例将用户按照职业归类，然后打印出来看，输出结果如下图所示(职业ID,((用户ID,用户性别,用户年龄),职业名称)：

![Screen Shot 2018-12-27 at 2.38.09 PM](/Users/hython/Desktop/Screen Shot 2018-12-27 at 2.38.09 PM.png)

完整代码附在这里：

```scala
package com.hython.spark.cores

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet
import org.apache.log4j.{Level, Logger}


object Hython_Analyzer_RDD {
  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);
    var masterUrl = "local[4]" //默认程序运行在本地Local模式中，主要学习和测试；
    var dataPath = "data/moviedata/medium/"  //数据存放的目录；

    /**
      * 当我们把程序打包运行在集群上的时候一般都会传入集群的URL信息，在这里我们假设如果传入
      * 参数的话，第一个参数只传入Spark集群的URL第二个参数传入的是数据的地址信息；
      */
    if(args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }
    //上面的这部分代码在本机测试时不需要用到
    val sc = new SparkContext(new SparkConf().setMaster(masterUrl).setAppName("Hython_Analyzer_RDD"));
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")

    /**
      * 首先用用户的职业来分类用户，记录用户的ID，Gender, Age
      */
    val usersBasic = usersRDD.map(_.split("::")).map{user => (//UserID::Gender::Age::OccupationID
      user(3),
      (user(0),user(1),user(2))
    )
    }
    val occupations = occupationsRDD.map(_.split("::")).map(job => (job(0), job(1)))

    val userInformation = usersBasic.join(occupations)

    userInformation.cache()

    for (elem <- userInformation.collect()) {
      println(elem)
    }

  }
}

```

有了上面的基础，我们还可以做一点点改进，只是为了更熟悉Scala语言，我们针对一部电影(ID为1193)，按照用户ID进行归类,Value的第一部分是电影和这个用户对该电影的评分，Value的第二部分是前面定义的UserInformation.

```scala
    //targetMovie的Key是userID,Value是MovieID
    val targetMovie = ratingsRDD.map(_.split("::")).map(x => (x(0), (x(1),x(2)) )).filter(_._2._1.equals("1193"))
    //(11,((4882,M,45),lawyer))
    val targetUsers = userInformation.map(x => (x._2._1._1, x._2))

    val userInformationForSpecificMovie = targetMovie.join(targetUsers)
    for (elem <- userInformationForSpecificMovie.collect()) {
      println(elem)
    }
```

部分输出结果如下：

> (3492,((1193,2),((3492,M,35),executive/managerial)))
> (4286,((1193,5),((4286,M,35),technician/engineer)))
> (5317,((1193,5),((5317,F,25),other or not specified)))
> (5975,((1193,5),((5975,M,25),sales/marketing)))
> (792,((1193,4),((792,M,25),technician/engineer)))

到目前为止，只用过Map和Join这两个功能，Spark RDD还有一系列的算子，Reduce, Sort是最基础也最重要的两个，在下面的例子里，我们来做一个电影流行度的分析，打印出所有电影中评分最高的前30个电影名和平均评分。

```scala
    /**
      * 电影流行度分析：所有电影中平均得分最高（口碑最好）的电影及观看人数最高的电影（流行度最高）
      * "ratings.dat"：UserID::MovieID::Rating::Timestamp
      * 得分最高的Top10电影实现思路：如果想算总的评分的话一般肯定需要reduceByKey操作或者aggregateByKey操作
      *   第一步：把数据变成Key-Value，把MovieID设置成为Key，把Rating设置为Value；
      *   第二步：通过reduceByKey操作或者aggregateByKey实现聚合
      *   第三步：排序，进行Key和Value的交换
      *
      *  注意：
      *   1，转换数据格式的时候一般都会使用map操作，有时候转换可能特别复杂，需要在map方法中调用第三方jar或者so库；
      *   2，RDD从文件中提取的数据成员默认都是String方式，需要根据实际需要进行转换格式；
      *   3，RDD如果要重复使用，一般都会进行Cache
      *   4，注意事项，RDD的cache操作之后不能直接再跟其他的算子操作，否则在一些版本中cache不生效
      */
    println("所有电影评分中平均得分最高(口碑最好)的电影：")
    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0), x(1), x(2))).cache()
    //格式化为Key, Value 形式 movieID -> Rating, 1
    ratings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) =>(x._1+y._1,x._2+y._2))
      .map(x => (x._2._1.toDouble / x._2._2, (x._1, x._2._2)))
      .sortByKey(false)
      .take(30)
      .foreach(println)

      
```

这里使用了求平均分来进行排名，我们从输出结果里很容易看到这个排名是有问题的，这也是因为策略问题导致数据质量下降的，很多平均分很高的电影是因为看过电影的人少，排名前十的片子只有一个人看，他给出了5.0，使用平均分的方法排名就会让这类电影排名很高。

输出是这样的：

> 所有电影评分中平均得分最高(口碑最好)的电影：
> (5.0,(989,1))
> (5.0,(787,3))
> (5.0,(3656,1))
> (5.0,(3881,1))
> (5.0,(1830,1))
> (5.0,(3382,1))
> (5.0,(3607,1))
> (5.0,(3280,1))
> (5.0,(3233,2))
> (5.0,(3172,1))

也许使用下面这种策略排名会更好

```scala
    println("所有电影中粉丝或者观看人数最多的电影:")
    ratings.map(x => (x._2, 1))
	  .reduceByKey(_+_).map(x => (x._2, x._1)).sortByKey(false)
      .take(10).foreach(println)
```

从ratings里我们无法知道用户的性别信息，用户的性别是在users.dat中的，如果我们想根据性别分别选出男生最喜欢的十部电影和女生最喜欢的十部电影，很自然的就会想到使用join. 这要怎么实现呢，我在下面给出我实现的代码。

```scala
//首先我们需要moviesInfo来把moviesRDD中的数据取出来    
val moviesInfo = moviesRDD.map(_.split("::")).map( x => (x(0),x(1))).cache()

    val usersGender = usersRDD.map(_.split("::")).map(x => (x(0), x(1)))
    val genderRatings = ratings.map(x => (x._1, (x._1,x._2,x._3))).join(usersGender).cache()
    genderRatings.take(10).foreach(println)
    //分别过滤出男性和女性的记录进行处理
    val maleFilteredRatings = genderRatings
		.filter(x => x._2._2.equals("M")).map(x => x._2._1)
    val femaleFilteredRatings = genderRatings
		.filter(x => x._2._2.equals("F")).map(x =>x._2._1)
    println("所有电影中最受男性喜爱的电影TOP10")
    maleFilteredRatings.map(x => (x._2,x._3))
		.reduceByKey( (x,y) => (x+y)).sortBy( x=> x._2).join(moviesInfo)
        .map(x => (x._1, x._2._2))
      	.take(10).zipWithIndex.foreach(x=>{
       	 	val t = (x._1._1,x._1._2,x._2 +1)
        	println(t)
   		})
	println("所有电影中最受女性喜爱的电影TOP10")
    femaleFilteredRatings.map(x => (x._2,x._3))
		.reduceByKey( (x,y) => (x+y)).sortBy( x=> x._2).join(moviesInfo)
      	.map(x => (x._1, x._2._2))
     	.take(10).zipWithIndex.foreach(x=>{
      		val t = (x._1._1,x._1._2,x._2 +1)
      		println(t)
    	})
```

我在编写程序时，想输出每部电影的排名，但是foreach又是不带排名的，所以我使用了zipWithIndex。以及我又想让排名从1开始而不是0开始，在take(10)之后的两段代码，展现了foreach的灵活性。

得到的输出如下：

> 所有电影中最受男性喜爱的电影TOP10
> (2828,Dudley Do-Right (1999),1)
> (2350,Heart Condition (1990),2)
> (3492,Son of the Sheik, The (1926),3)
> (736,Twister (1996),4)
> (3638,Moonraker (1979),5)
> (1245,Miller's Crossing (1990),6)
> (312,Stuart Saves His Family (1995),7)
> (3319,Judy Berlin (1999),8)
> (3283,Minnie and Moskowitz (1971),9)
> (2329,American History X (1998),10)
> 所有电影中最受女性喜爱的电影TOP10
> (2828,Dudley Do-Right (1999),1)
> (2350,Heart Condition (1990),2)
> (3492,Son of the Sheik, The (1926),3)
> (736,Twister (1996),4)
> (3638,Moonraker (1979),5)
> (1245,Miller's Crossing (1990),6)
> (312,Stuart Saves His Family (1995),7)
> (3319,Judy Berlin (1999),8)
> (3283,Minnie and Moskowitz (1971),9)
> (2329,American History X (1998),10)

通过上面的这个简单的小程序，我们可以看到Spark的编程，使用高阶函数，用起来十分方便；编写分布式的程序像编写单机程序一样，Spark是MapReduce计算框架的替代品，在任何规模的数据计算中，Spark在性能和扩展性上都更具优势。

### 三、Spark 2.2的DataFrame和DataSet (小小对比Java和Scala)

在分析Spark的源代码之前，我们再来看两个简化程序员工作的好东西，分别是DataFrame和DataSet.

#### 3.1 DataFrame

DataFrame类似于传统数据库中的二维表格。DataFrame与RDD的主要区别在于，前者带有schema的元信息，即DataFrame表示的二维表数据集的每一列都带有名称和类型。这使得Spark SQL得以解析到具体数据的结构信息，从而对DataFrame中的数据源以及对DataFrame的操作进行了非常有效的优化，从而大幅提升了运行效率。

#### 3.2 DataSet

DataSet是强类型的，而DataFrame实际上是DataSet\[Row\]\(也就是Java中的DataSet\<Row\>)。DataSet是Lazy级别的，Transformation级别的算子作用于DataSet会得到一个新的DataSet。当Action算子被调用时，Spark的查询优化器会优化Transformation算子形成的逻辑计划，并生成一个物理计划，该物理计划可以通过并行和分布式方法来执行。

DataSet底层仍然是Spark RDD, 但由于RDD没有数据元素的具体结构，很难被Spark自身进行优化，对新手用户很不友好。但是从原理角度看，优化引擎做不到的地方，用户在RDD上可能有机会进行手动优化。

#### 3.3 通过DataFrame实现电影点评系统

DataFrame是基于SparkSession的，可以用SQL语句进行操作，话不多说，来看下我的这个例子。

```scala
    println("Spark SQL 1.通过DataFrame实现某部电影观看者中男性和女性不同年龄人数")
    //首先把Users数据格式化
    val schemaForUsers = types.StructType("UserID::Gender::Age::OccupationID::Zip-code".split("::")
      .map(column => types.StructField(column, types.StringType, true)))
    //把每一条数据变成以Row为单位的数据
    val usersRDDRows = usersRDD
      .map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim,line(3).trim,line(4).trim))

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .getOrCreate()

    val usersDataFrame = spark.createDataFrame(usersRDDRows, schemaForUsers)

    //构建Rattings的DataFrame
    val schemeForRattings = types.StructType("UserID::MovieID".split("::")
      .map(column => types.StructField(column,types.StringType,true)))
        .add("Ratings",types.DoubleType, true)
        .add("Timestamps", types.StringType, true)

    val ratingsRDDRows = ratingsRDD.map(_.split("::"))
      .map(line => Row(line(0).trim,line(1).trim,line(2).trim.toDouble,line(3).trim))

    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows, schemeForRattings)


    //构建Movies的DataFrame
    val schemaForMovies = types.StructType("MovieID::Title::Genres".split("::")
      .map(column => types.StructField(column, types.StringType, true)))

    val moviesRDDRows = moviesRDD.map(_.split("::"))
      .map(line => Row(line(0).trim,line(1).trim,line(2).trim))

    val moviesDataFrame = spark.createDataFrame(moviesRDDRows, schemaForMovies)

    //进行一些简单的处理
    ratingsDataFrame.filter(s"MovieID = 1193")
      .join(usersDataFrame, "UserID")
      .select("Gender","Age")
        .groupBy("Gender","Age")
      .count().show(10)

    //上面这个例子和SQL非常相似，为什么叫Spark SQL呢，我们用SQL语句来写一下
    println("Spark SQL 2. 用LocalTempView实现某部电影观看者中不同性别不同年龄分别有多少人？")
    ratingsDataFrame.createTempView("ratings")
    usersDataFrame.createTempView("users")
    //执行SQL语句
    val sql_local = "SELECT Gender, Age, count(*) from users As u join " +
      "ratings As r on u.UserID = r.UserID where MovieID = 1193 group by Gender, Age"
    spark.sql(sql_local).show(10)

	//要使用avg这样的函数，我们还需要import implicits
	import spark.sqlContext.implicits._
    ratingsDataFrame.select("MovieID","Rating")
      .groupBy("MovieID").avg("Rating")
      .orderBy($"avg(Rating)".desc).show(10)
	//$用来将字符串转化成列来实现更复杂的功能
```

#### 3.4 用Java 实现的一个“简单”的程序（二次排序）

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class MovieUsersAnalyzer {
    public static  void main(String[] args){
        /**
         * 创建Spark集群上下文sc
         */
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[4]").setAppName("Movie_Users_Analyzer"));

//        JavaRDD<String> lines = sc.textFile("data/moviedata/medium/" + "dataforsecondarysorting.txt");
        //"ratings.dat"：UserID::MovieID::Rating::Timestamp
        JavaRDD<String> lines = sc.textFile("data/moviedata/medium/" + "ratings.dat");

        JavaPairRDD<SecondarySortingKey, String> keyvalues = lines.mapToPair(new PairFunction<String, SecondarySortingKey, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<SecondarySortingKey, String> call(String line) throws Exception {
                String[] splited = line.split("::");
                SecondarySortingKey key = new SecondarySortingKey(Integer.valueOf(splited[3]),
                        Integer.valueOf(splited[2]));
                return new Tuple2<SecondarySortingKey, String>(key, line);
            }
        });
        JavaPairRDD<SecondarySortingKey, String> sorted = keyvalues.sortByKey(false);

        JavaRDD<String> result = sorted.map(new Function<Tuple2<SecondarySortingKey,String>, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(Tuple2<SecondarySortingKey, String> tuple) throws Exception {
                return tuple._2;
            }
        });

        List<String> collected = result.take(10);
        for(String item : collected){
            System.out.println(item);
        }
    }
}
```

这个程序实现的是对ratings按timestamps排序，timestamps相同的按rating排序，将排完序的前十个按顺序输出原始的行。

除此之外还需要一个用来实现二次排序的类，用java实现非常繁琐，完整代码如下所示：

```java
//package xxx

import scala.Serializable;
import scala.math.Ordered;

public class SecondarySortingKey implements Ordered<SecondarySortingKey>, Serializable{
    private int first;
    private int second;

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }



    public SecondarySortingKey(int first, int second){
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondarySortingKey that = (SecondarySortingKey) o;

        if (first != that.first) return false;
        return second == that.second;

    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }

    @Override
    public int compare(SecondarySortingKey that) {
        if(this.first - that.getFirst() != 0){
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

    @Override
    public boolean $less(SecondarySortingKey that) {
        if(this.first < that.getFirst()) {
            return  true;
        } else if (this.first == that.getFirst() &&this.second < that.getSecond()){
            return true;
        }

        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortingKey other) {
        if(SecondarySortingKey.this.$less(other)){
            return true;
        } else if (this.first == other.getFirst() && this.second == other.getSecond()){
            return true;
        }

        return false;
    }

    @Override
    public boolean $greater(SecondarySortingKey that) {
        if(this.first > that.getFirst()){
            return true;
        }else if(this.first == that.getFirst() && this.second > that.getSecond()) {
            return true;
        }

        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortingKey that) {
        if (SecondarySortingKey.this.$greater(that)){
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()){
            return true;
        }

        return false;
    }

    @Override
    public int compareTo(SecondarySortingKey that) {
        if(this.first - that.getFirst() != 0){
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }
}

```



输出结果是这样的

> 4958::1924::4::1046454590
> 4958::3264::4::1046454548
> 4958::2634::3::1046454548
> 4958::1407::5::1046454443
> 4958::2399::1::1046454338
> 4958::3489::4::1046454320
> 4958::2043::1::1046454282
> 4958::2453::4::1046454260
> 5312::3267::4::1046444711
> 5948::3098::4::1046437932

### 3.5 用Scala实现刚刚Java实现的二次排序，做相同的事

用Scala完成会简单很多，我们的主程序如下所示

```scala
object SecondarySortApp {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val conf = new SparkConf().setAppName("SecondarySortApp").setMaster("local[4]") //创建SparkConf，初始化程序的配置
    val sc = new SparkContext(conf) //创建SparkContext，这是第一个RDD创建的唯一入口，也是Driver的灵魂，是通往集群的唯一通道

    val lines = sc.textFile("data/moviedata/medium/" + "ratings.dat")

    val  pairWithSortKey = lines.map(line => {
      val splited = line.split("::")
      (new SecondarySortKey(splited(3).toDouble, splited(2).toDouble), line)
    })

    val sorted = pairWithSortKey.sortByKey(false)

    val sortedResult = sorted.map(sortedLine => sortedLine._2)

    sortedResult.collect().take(10).foreach(println)
  }

}


```

实现二次排序的代码如下所示

```scala
class SecondarySortKey(val first:Double, val second: Double)
extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if(this.first - that.first !=0 ){
      (this.first-that.first).toInt
    }else {
      //如果第一个字段相等，则比较第二个字段
      if(this.second - that.second > 0) {
        Math.ceil(this.second - that.second).toInt
      }else if(this.second - that.second < 0){
        Math.floor(this.second - that.second).toInt
      }else{
        (this.second-that.second).toInt
      }
    }
  }
}

```

可以看到，Scala只用了40行就完成了Java需要一百多行的任务，而且语法更加和任务相关，编程时不需要繁冗的信息，因此我们建议在我们的项目中也使用Scala进行编程。

### 四、Spark 2.2 的流处理 Structured Streaming

我们的项目要处理流数据，自Spark流行以来， Spark Streaming逐渐吸引了很多用户，得益于其易用的高级API和一次性语义，成为最广泛的流处理框架之一。

Spark 2.0最重磅的更新是新的流处理框架——**Structured Streaming**. 它允许用户使用DataFrame/DataSetAPI 编写与离线批处理几乎相同的代码，便可以作用到流数据和静态数据上，引擎会自动增量化流数据计算，同时保证了数据处理的一致性。

Spark 2.0中添加了很多的更新，其中我们的项目也许最关心的是下面这两个更新

> SPARK-20844: 结构化流式API现在已经是可行的，不再被标注为实验性
>
> SPARK-19719: 支持从Apache Kafka流式传输或批量读取和写入数据

下面结合官方文档来了解一下Structured Streaming

### 4.1 一个WordCount 程序

首先我们来看不使用流处理的情况，我们读取一个文档（在我的电脑上，这个文档是用Novel的Wikipedia网页，我把所有内容复制到Text Edit里。有了前几节的经验，这个工作变得非常简单，不过我仍然根据文档写了非常详尽的注释。以后如果需要将Spark部署到服务器上，可以参考这个简单的程序，作为第一个测试案例。只需要setMaster到Master节点上，然后读取文件的时候从hdfs中读就可以很快速的把这个程序部署到服务器了。

```scala
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main (args: Array[String]) {
    /**
      * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
      */
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("WordCount") //设置应用程序的名称，在程序运行的监控界面可以看到名称
//    conf.setMaster("spark://Master:7077") //此时，程序在Spark集群
    conf.setMaster("local[4]")
    /**
      * 第2步：创建SparkContext对象
      * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须有一个SparkContext
      * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend
      * 同时还会负责Spark程序往Master注册程序等
      * SparkContext是整个Spark应用程序中最为至关重要的一个对象
      */
    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息
    /**
      * 第3步：根据具体的数据来源（HDFS、HBase、Local FS、DB、S3等）通过SparkContext来创建RDD
      * RDD的创建基本有三种方式：根据外部的数据来源（例如HDFS）、根据Scala集合、由其它的RDD操作
      * 数据会被RDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
      */

//    val lines = sc.textFile("hdfs://Master:9000/library/wordcount/input/Data") //读取HDFS文件并切分成不同的Partions
//    val lines = sc.textFile("/library/wordcount/input/Data") //读取HDFS文件并切分成不同的Partions
    val lines = sc.textFile("/Users/hython/Desktop/NovelWiki.rtfd") //读取本地文件并设置为一个Partion
    /**
      * 第4步：对初始的RDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
      * 	第4.1步：讲每一行的字符串拆分成单个的单词
      */

    val words = lines.flatMap { line => line.split(" ")} //对每一行的字符串进行单词拆分并把所有行的拆分结果通过flat合并成为一个大的单词集合
    "Spark".format()
    /**
      * 第4步：对初始的RDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
      * 	第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
      */
    val pairs = words.map { word => (word, 1) }

    /**
      * 第4步：对初始的RDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
      * 	第4.3步：在每个单词实例计数为1基础之上统计每个单词在文件中出现的总次数
      */
    val wordCountsOdered = pairs.reduceByKey(_+_).map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1)) //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）

    wordCountsOdered.cache()
    //wordCountsOdered.count()
    //wordCountsOdered.take(10)

    wordCountsOdered.collect.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))

    //while(true){}

    sc.stop()
  }
}

```

如果是读取流数据，需要作出一些修改，在创建Session以后的第二步，创建流

```scala
	val lines = spark.readStream.format("socket")
				.option("host","localhost").option("port","9999")
				.load()

```

第三步是进行单词统计

```scala
	val words = lines.as[String].flatMap(_.split(" "))
	val wordcount = words.groupBy("values").count()

```

第四步创建查询句柄，定义打印结果方式并启动程序。

```scala
	val query = wordcount.writeStream
		.outputMode("complete").format("console").start()
	query.awaitTermination()

```



Structured Streaming 共有3种输出模式，都只适用于某些类型的查询

1. CompleteMode: 完整模式。整个更新的结果表将被写入外部存储器。由存储连接器决定如何处理整张表的写入。聚合操作以及聚合操作之后的排序操作支持这种模式。
2. AppendMode: 附加模式。只有自上次触发执行后在结果表中附加的新行会被写入外部存储器。这仅适用于结果表中现有行不会更改的查询，如select, where, map, flatMap, filter, join等操作支持这种模式。
3. UpdateMode: 更新模式(这个模式将在以后的版本出现)

### 4.2 对实时数据的支持

来看下基础的窗口函数在Structured Streaming 中的应用。要对10min内的单词技术，每5min统计一次。具体代码如下：

```scala
val windowCounts = words
	.groupBy(
		window($"timestamp","10 minutes","5 minutes"),word
	).count

```

在Spark 2.1中引入的水印(watermarking)，使得系统可以自动跟踪目前的event-time，并且按照用户指定的时间清理旧的状态。使用方法如下：

```scala
val windowedCounts = words
	.withWatermark("timestamp", "10 minutes")
	.groupBy(window($"timestamp","10 minutes", "5 minutes"),$"word")
	.count()

```

