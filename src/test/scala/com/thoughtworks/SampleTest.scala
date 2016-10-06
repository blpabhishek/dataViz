package com.thoughtworks

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SampleTest extends FunSuite with BeforeAndAfterEach {

    var sparkContext: SparkContext = null


    override def beforeEach() {
        val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        sparkContext = new SparkContext(sparkConf)
    }

    override def afterEach() {
        sparkContext.stop()
    }

    test("should not fail") {
        //assert(1 == Sample.identity(1))
    }
}
