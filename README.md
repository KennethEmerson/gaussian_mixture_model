# gaussian_mixture

This SBT project implements an Expectation– Maximization algorithm that allows for the classification of a univariate multi-modal Gaussian Mixture Model using the Scala API of the Apache Spark cluster computing framework. 
The implementation of this unsupervised learning model allows us to determine the weights, means and standard deviations of the Gaussian distributed subpopulations in an unclassified one-dimensional dataset of numbers and uses the standard RDD data structures provided in Spark.

In order to confirm that the algorithm is implemented correctly, a dummy dataset of 2000 numbers is added, consisting of four subpopulations with the following characteristics:

| Subpopulation | mean  | Standard deviation | Number of values (weight) |
| :-------------: | :-----: | :-------------------: | :---------------------------:|
| 1 | 100 | 10 | 200 |
| 2 | 200 | 20 | 800 |
| 3 | 300 | 40 | 600 |
| 4 | 150 | 25 | 400 |

For more info on the Gaussian mixture model please consult:
https://brilliant.org/wiki/gaussian-mixture-model/

