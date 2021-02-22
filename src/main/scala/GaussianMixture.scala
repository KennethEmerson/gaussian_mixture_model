import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.{sqrt,Pi,exp,pow,log}
import java.util.Calendar


/**
 * object to calculate the Gaussian mixture of a one-dimensional numeric dataset for an arbitrary k set of
 * Gaussian distributions
 *version: 24JAN2021
 *
 */
object GaussianMixture {

  /**
   * case classes containing the k values of the means, standard deviations or weights.
   * allows for type checking in the code
   * @param values: Array of size K containing the values for the means, standard deviations or weights
   */
  case class KWeights(values: Array[Double])
  case class KMeans(values: Array[Double])
  case class KStds(values: Array[Double])

  /**
   * case classes containing the actual number from the dataset and
   * the corresponding k values of the weighted probabilities or gammas.
   *
   * @param number: the actual number from the dataset
   * @param values: Array of size K containing the weighted probabilities of gammas
   */
  case class KWeightProbs(number: Double,values: Array[Double])
  case class KGammas(number: Double, values: Array[Double])

  /**
   * case class containing the actual parameters after one iteration
   * @param kMeans: KMeans object containing the means for the k distributions
   * @param kStds: KStds object containing the standard deviations for the k distributions
   * @param kWeights: KWeights object containing the weights for the k distributions
   */
  case class Parameters(kMeans: KMeans, kStds: KStds, kWeights: KWeights)

  /**
   * case class containing the end result of the gaussian mixture
   * overrides the toString method to allow the printing of the results on the terminal
   * @param counter: the number of iterations performed before reaching the loglikelihood delta treshold
   * @param datasize: size of the dataset
   * @param treshold: minimal delta between loglikelihoods to stop iterating
   * @param logLikelihood: the final loglikelihood
   * @param kMeans: KMeans object containing the final means of the k distributions
   * @param kStds: KStds object containing the final standadr deviations of the k distributions
   * @param kWeights: KWeights object containing the final weights of the k distributions
   */
  case class Result(counter: Int,
                    datasize:Long,
                    treshold:Double,
                    logLikelihood: Double,
                    kMeans: KMeans,
                    kStds: KStds,
                    kWeights: KWeights ){

    def round(value:Double,decimals:Int=3):Double = {
      math.floor(value * pow(10,decimals)) / pow(10,decimals)
    }
    override def toString: String = {
      var string = s"\ncycle count: $counter" +
                   s"\ndatasize: $datasize" +
                   s"\ntreshold: $treshold" +
                   s"\nloglikelihood: $logLikelihood\n"
      for(i <- kMeans.values.indices){
          string = string + s"\nsubpopulation $i:\t" +
            s"(mean: ${round(kMeans.values(i))}\t" +
            s"stand dev: ${round(kStds.values(i))}\t" +
            s"weight: ${round(kWeights.values(i))})"
      }
      string
    }
  }

  /**
   * calculate the weighted probability of observing x, given a single gaussian distribution
   *  with a given mean,standard deviation and weight
   * @param x: value for which weighted probability is to be calculated
   * @param mean: the given mean for the distribution
   * @param std: the given standard deviation for the distribution
   * @param weight: the given weight for the distribution
   * @return the weighted distribution
   */
  def calcWeightedProbability(x: Double,mean: Double,std:Double,weight:Double):Double = {
    weight*(1/(std*sqrt(2*Pi)))*exp(-(pow(x-mean,2)/(2*pow(std,2))))
  }

  /**
   * calculate the weighted probabilities of observing x, given multiple gaussian distributions
   * @param number: value for which weighted probabilities is to be calculated
   * @param kMeans: array of size k containing the means for the k distributions
   * @param kStds: array of size k containing the standard deviations for the k distributions
   * @param kWeights: array of size k containing the weights for the k distributions
   * @return object of type KWeightProbs containing the actual number and its weighted probabilities
   */
  def calcKWProbs(number:Double,kMeans:KMeans,kStds:KStds,kWeights:KWeights):KWeightProbs ={
    val initArray = Array.fill[Double](kMeans.values.length)(number)
    val kWProbs = initArray.zipWithIndex.map({case (number,index) =>
                          calcWeightedProbability(number,kMeans.values(index),kStds.values(index),kWeights.values(index))})
    KWeightProbs(number,kWProbs)
  }

  /**
   * calculate the weighted probabilities of all values in a univariate RDD, given k gaussian distributions
   * @param dataRDD: RDD containing the univariate dataset of numbers
   * @param kMeans: KMeans object containing the means for the k distributions
   * @param kStds: KStds object containing the standard deviations for the k distributions
   * @param kWeights: KWeights object containing the weights for the k distributions
   * @return RDD containing the KWeightProbs objects for each number in the dataset
   */
  def getKWProbRDD(dataRDD:RDD[Double],kMeans:KMeans,kStds:KStds,kWeights:KWeights):RDD[KWeightProbs] = {
    dataRDD.map(number => calcKWProbs(number,kMeans,kStds,kWeights))
  }

  /**
   * calculates the gamma values given a KWeightProbs object with the number and weighted probabilities
   * @param kWeightProbs: KWeightProbs object containing the number and weighted probabilities
   * @return KGammas object containing the number and its calculated k gamma values
   */
  def calcKGammaValues(kWeightProbs:KWeightProbs):KGammas = {
    val number = kWeightProbs.number
    val denominator = kWeightProbs.values.sum
    val kGammas = kWeightProbs.values.map(x => x/denominator)
    KGammas(number,kGammas)
  }

  /**
   * calculate the gamma values of all numbers in a univariate RDD, given the weighte probabilities
   * @param kWProbRDD: RDD containing the KWeightProbs objects for each number in the dataset
   * @return RDD containing the KGammas objects for each number in the dataset
   */
  def getGammaRDD(kWProbRDD:RDD[KWeightProbs]): RDD[KGammas]={
    kWProbRDD.map(element => calcKGammaValues(element))
  }

  /**
   * calculates element wis sum of RDD containing arrays
   * @param k size of the arrays in the RDD
   * @param arrayRDD: RDD containing the arrays
   * @return Array containing the sums of every element
   */
  def sumElementwise(k:Int,arrayRDD:RDD[Array[Double]]):Array[Double] ={
    val initKSumOfGammas = Array.fill(k)(0.0)
    arrayRDD.fold(initKSumOfGammas)((a,b)=>a.zip(b).map { case (x, y) => x + y })
  }

  /**
   * multiplies the gamma values with the number in the KGamma object
   * @param kGammas: KGamma object containing the number and its correpsonding k gamma values
   * @return Array of resulting values
   */
  def multiplykGammasWithNumber(kGammas:KGammas):Array[Double] = {
    val number = kGammas.number
    kGammas.values.map(gamma => gamma*number)
  }

  /**
   * calculates the nominator for the variance formula of one number
   * @param kGammas: KGamma object containing the number and its corresponding k gamma values
   * @param kMeans: KMeans object containing the means for the k distributions
   * @return nominator value of the variance formula for one number
   */
  def calcKVarianceNominator(kGammas:KGammas,kMeans:KMeans):Array[Double] = {
    val number = kGammas.number
    kGammas.values.zipWithIndex.map({case (gamma,index) => gamma*pow(number-kMeans.values(index),2)})
  }

  /**
   * calculates the loglikelihood of the dataset given the RDD with the weighted probabilities
   * @param kWProbRDD: RDD containing the KWeightProbs objects for each number in the dataset
   * @return the loglikelihood of the dataset
   */
  def calcLogLikelihood(kWProbRDD: RDD[KWeightProbs]):Double = {
    kWProbRDD.map(element => log(element.values.sum)).sum
  }

  /**
   * updates the means, standard deviations and weights of the K components
   * @param k: number of presumably gaussian distributions in the dataset
   * @param datasize: number of values in the univariate dataset
   * @param gammasRDD: RDD containing the KGammas objects for each number in the dataset
   * @return Parameter object containing the actual parameters after an iteration
   */
  def updateDistrParameters(k:Int,datasize:Long,gammasRDD:RDD[KGammas]):Parameters= {
    val kSumOfGammas:Array[Double] = sumElementwise(k,gammasRDD.map(element =>element.values))

    val kWeights:KWeights = KWeights(kSumOfGammas.map(x => x/datasize))

    val kMeansNominators:Array[Double] = sumElementwise(k,gammasRDD.map(x => multiplykGammasWithNumber(x)))
    val kMeans:KMeans = KMeans(kSumOfGammas.zipWithIndex.map({case (sum,index) => kMeansNominators(index)/sum}))

    val kStdNominators:Array[Double] = sumElementwise(k,gammasRDD.map(x => calcKVarianceNominator(x,kMeans)))
    val kStds:KStds = KStds(kSumOfGammas.zipWithIndex.map({case (sum,index) => sqrt(kStdNominators(index)/sum)}))

    Parameters(kMeans,kStds,kWeights)

  }

  /**
   * determines the means, standard deviations and weights of the presumably K components in the
   * univariate dataset, based on the optimalisation of the loglikelihood
   * @param dataRDD: RDD containing the univariate dataset of numbers
   * @param k: number of presumably Gaussian distributions in the dataset
   * @param treshold: minimum improvement of the loglikelihood between iterations
   * @return result object containing the means, standard deviations and weights of the K components
   */
  def expMax(dataRDD:RDD[Double],k:Int,treshold:Double):Result = {

    //initialize parameters
    val dataSize:Long = dataRDD.count()
    val kMeansInit:KMeans = KMeans(dataRDD.takeSample(withReplacement = false,k))
    val kStdsInit:KStds = KStds(Array.fill(k)(dataRDD.stdev()))
    val kWeightsInit:KWeights = KWeights(Array.fill(k)(1.0/k))
    var parameters = Parameters(kMeansInit,kStdsInit,kWeightsInit)

    var counter = 0
    var continue = true
    // calculate initial likelihood
    var kWProbRDD = getKWProbRDD(dataRDD,parameters.kMeans,parameters.kStds,parameters.kWeights)
    var newLogLikelihood:Double = calcLogLikelihood(kWProbRDD)

    while(continue){
      val oldLogLikelihood = newLogLikelihood
      counter = counter + 1

      val gammaRDD = getGammaRDD(kWProbRDD).persist()
      kWProbRDD.unpersist()

      parameters = updateDistrParameters(k,dataSize,gammaRDD)
      gammaRDD.unpersist()

      kWProbRDD = getKWProbRDD(dataRDD,parameters.kMeans,parameters.kStds,parameters.kWeights).persist()
      newLogLikelihood = calcLogLikelihood(kWProbRDD)
      continue = math.abs(newLogLikelihood-oldLogLikelihood) > treshold

    }
    Result(counter,dataSize,treshold,newLogLikelihood,parameters.kMeans,parameters.kStds,parameters.kWeights)
  }

/*************************************************************************************************/
  /**
   * The main method creates the Spark configuration and the corresponding Spark context,
   * defines the variables for the expectation maximization algorithm, loade the dataset from file,
   * runs the algorithm and returns the result as a result object.
   */
  def main(args:Array[String]):Unit={
    val t0 = Calendar.getInstance.getTimeInMillis
    val conf=new SparkConf()
    conf.setAppName("GMM")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //select dataset
    
    val datafile = "dataset_validation.txt"

    //variable settings
    val k = 4
    val deltaTreshold = 0.1

    //load data
    val dataRDD = sc.textFile(datafile).map(s=> s.mkString.toDouble).persist()

    val result = expMax(dataRDD,k,deltaTreshold)
    val t1 = Calendar.getInstance.getTimeInMillis
    val duration = t1 - t0

    println(result)
    println(s"time duration (ms): $duration")
    sc.stop()

  }
}

