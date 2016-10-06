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
        val header: String = textFile.first()
        val dataWithoutHeader = textFile.subtract(sc.parallelize(Array(header)))
        val rdd: TransformableRDD = new TransformableRDD(dataWithoutHeader)
        val sample: TransformableRDD = rdd.removeRows((record: RowRecord) => record.hasEmptyColumn)
        sample.sample(withReplacement = true, 0.005, 1).saveAsTextFile("data/sample")
        sample.saveAsTextFile("data/remove")
    }

    def main(args: Array[String]): Unit = {
        alienAggregation()

    }

    def alienAggregation(): Unit = {
        val sc: SparkContext = setContext()
        val textFile: RDD[String] = sc.textFile("data/alien.csv")
        val header: String = textFile.first()
        val dataWithoutHeader = textFile.subtract(sc.parallelize(Array(header)))
        val rdd: TransformableRDD = new TransformableRDD(dataWithoutHeader)
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