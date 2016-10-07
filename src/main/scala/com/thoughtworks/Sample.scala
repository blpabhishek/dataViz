package com.thoughtworks

import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import com.thoughtworks.datacommons.prepbuddy.types.CSV
import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sample {

    def setContext() = {
        val conf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local[*]")
        new SparkContext(conf)
    }

    def preprocess(): Unit = {
        val sc: SparkContext = setContext()
        val textFile: RDD[String] = sc.textFile("data/RawStackOverflowData.csv")
        val rdd: TransformableRDD = removeHeader(sc, textFile)

        val sample: TransformableRDD = rdd.removeRows((record: RowRecord) => record.hasEmptyColumn)
        sample.sample(withReplacement = true, 0.005, 1).saveAsTextFile("data/sample")
        sample.saveAsTextFile("data/remove")
    }

    def filterOutlier(): Unit = {
        val sc: SparkContext = setContext()
        val textFile: RDD[String] = sc.textFile("data/avg.csv")
        val rdd: TransformableRDD = removeHeader(sc, textFile)

        rdd.removeRows((record: RowRecord) => {
            record.select(2).toInt < 5
        }).saveAsTextFile("data/filterSal")
    }

    def main(args: Array[String]): Unit = {
        groupSalaryWise
    }

    def groupSalaryWise: Unit = {
        val sc: SparkContext = setContext()
        val textFile: RDD[String] = sc.textFile("data/countryWiseSalary")
        val rdd: TransformableRDD = removeHeader(sc, textFile)
        val filtered: TransformableRDD = rdd.removeRows((record: RowRecord) => record.hasEmptyColumn)
        filtered.map((string: String) => {
            val parse = CSV.parse(string)
            (parse.select(0), parse.select(1))
        })
            .groupByKey().map((tuple: (String, Iterable[String])) => {
            (tuple._1, tuple._2.map((s: String) => s.toInt).sum, tuple._2.size)
        }).saveAsTextFile("data/countryAndSal")
    }

    def removeHeader(sc: SparkContext, textFile: RDD[String]): TransformableRDD = {
        val header: String = textFile.first()
        val dataWithoutHeader = textFile.subtract(sc.parallelize(Array(header)))
        new TransformableRDD(dataWithoutHeader)
    }

    def alienAggregation(): Unit = {
        val sc: SparkContext = setContext()
        val textFile: RDD[String] = sc.textFile("data/alien.csv")
        val rdd: TransformableRDD = removeHeader(sc, textFile)
        val filtered: TransformableRDD = rdd.drop(2).removeRows((record: RowRecord) => record.hasEmptyColumn)
        filtered.map((string: String) => {
            val parse = CSV.parse(string)
            (parse.select(0), parse.select(1))
        }).groupByKey().map { case (country, response) =>
            val yes: Int = response.count((str: String) => str.equals("Yes"))
            val no: Int = response.count((str: String) => str.equals("No"))
            (country, yes, no, response.size - (yes + no))
        }.saveAsTextFile("data/alienFiltered")
    }
}