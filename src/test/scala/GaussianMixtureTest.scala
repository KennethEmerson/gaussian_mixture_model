import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import GaussianMixture._

class GaussianMixtureTest extends AnyFunSuite {

  def fixture: Object {
    val sc: SparkContext

    val conf: SparkConf
  } =
    new {
      val conf=new SparkConf()
      conf.setAppName("DatasetsTest")
      conf.setMaster("local[2]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("WARN")
    }

  val f = fixture

  test("calculate weighted probability of one value should give correct result") {
    val x: Double = 4
    val mean: Double = 8
    val std: Double = 2
    val weight: Double = 1

    calcWeightedProbability(x,mean,std,weight)  should === (0.026995483 +- 0.000000001)
  }

  test("calculate all weighted probabilities for one value should return KWeightProbs object") {
    val testKMeans = KMeans(Array(8,10,5))
    val testKStds = KStds(Array(2,3,1))
    val testKWeights = KWeights(Array(1,2,1))
    val number = 4

    val testkWeightProbs = calcKWProbs(number,testKMeans,testKStds,testKWeights)

    testkWeightProbs shouldBe a [KWeightProbs]
    testkWeightProbs.number shouldBe a [Double]
    testkWeightProbs.values shouldBe a [Array[_]]
    }

  test("calculate all weighted probabilities for one value should give correct results") {
    val testKMeans = KMeans(Array(8,10,5))
    val testKStds = KStds(Array(2,3,1))
    val testKWeights = KWeights(Array(1,2,1))
    val number = 5

    val testkWeightProbs = calcKWProbs(number,testKMeans,testKStds,testKWeights)
    testkWeightProbs.number should be (number)
    testkWeightProbs.values(0) should === (0.0647587978329459 +- 0.000000001)
    testkWeightProbs.values(1) should === (0.0663180925284991 +- 0.000000001)
    testkWeightProbs.values(2) should === (0.398942280401433 +- 0.000000001)
  }

  test("getKWProbRDD must return an RDD containing kWeightProbs") {

    val testdataRDD = f.sc.parallelize(Array(6.0,5.0,4.0,7.0))
    val testKMeans = KMeans(Array(8,10,5))
    val testKStds = KStds(Array(2,3,1))
    val testKWeights = KWeights(Array(1,2,1))
    val testkWeightProbRDD =getKWProbRDD(testdataRDD,testKMeans,testKStds,testKWeights)

    testkWeightProbRDD shouldBe a [RDD[_]]
    testkWeightProbRDD.count() should be (4)
    testkWeightProbRDD.first().values.length should be (3)
    testkWeightProbRDD.first().values(0) === (0.120985362259572 +- 0.000000001)
    testkWeightProbRDD.first().values(1) === (0.109340049783996 +- 0.000000001)
    testkWeightProbRDD.first().values(2) === (0.241970724519143 +- 0.000000001)


  }

  test("calculate gamma values from weighted probabilities") {
    val testKMeans = KMeans(Array(8,10,5))
    val testKStds = KStds(Array(2,3,1))
    val testKWeights = KWeights(Array(1,2,1))
    val number = 5

    val testkWeightProbs = calcKWProbs(number,testKMeans,testKStds,testKWeights)
    val testkGammas = calcKGammaValues(testkWeightProbs)

    testkGammas shouldBe a [KGammas]
    testkGammas.number should be (number)
    testkGammas.values(0) should === (0.12218199153011 +- 0.000000001)
    testkGammas.values(1) should === (0.12512395057908 +- 0.000000001)
    testkGammas.values(2) should === (0.75269405789081 +- 0.000000001)
  }

  test("getGammaRDD must return an RDD containing KGammas") {
    val testdataRDD = f.sc.parallelize(Array(7.0,5.0,4.0,6.0))
    val testKMeans = KMeans(Array(8,10,5))
    val testKStds = KStds(Array(2,3,1))
    val testKWeights = KWeights(Array(1,2,1))
    val testkWeightProbRDD =getKWProbRDD(testdataRDD,testKMeans,testKStds,testKWeights)
    val testkGammasRDD = getGammaRDD(testkWeightProbRDD)

    testkGammasRDD shouldBe a [RDD[_]]
    testkGammasRDD.count() should be (4)
    testkGammasRDD.first().values.length should be (3)
    testkGammasRDD.first().values(0) === (0.449823202642221 +- 0.000000001)
    testkGammasRDD.first().values(1) === (0.412211552703224 +- 0.000000001)
    testkGammasRDD.first().values(2) === (0.137965244654555 +- 0.000000001)
  }

  test("sumElementwise should return element wise sum of Arrays") {
    val testdataRDD = f.sc.parallelize(Array(Array(1.0,2.0,3.5),Array(2.0,3.0,4.5),Array(3.0,4.0,5.0)))
    val result = sumElementwise(3,testdataRDD)
    result should be (Array(6.0,9.0,13.0))
  }

  test("multiplykGammasWithNumber should multiply gamma values with the number") {
    val testKGamma = KGammas(5,Array(1,5,10))
    val result = multiplykGammasWithNumber(testKGamma)
    result should be (Array(5.0,25.0,50.0))
  }

  test("calcKStdNominator should return correct result") {
    val testKMeans = KMeans(Array(8,10,5))
    val testKGamma = KGammas(5,Array(1,5,10))
    val result = calcKVarianceNominator(testKGamma,testKMeans)
    result should be (Array(9.0,125.0,0))
  }

  test("calcLogLikelihood should return correct loglikelihood") {
    val kWP1 = KWeightProbs(5,Array(1.0,4.0,3.0))
    val kWP2 = KWeightProbs(5,Array(2.0,5.0,3.0))
    val kWP3 = KWeightProbs(5,Array(3.0,6.0,12.0))
    val testKWProbs = f.sc.parallelize(Array(kWP1,kWP2,kWP3))
    testKWProbs === (2.0050612922078 +- 0.000000001)
  }

}
