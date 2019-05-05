from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkConf, SparkContext
import itertools
import sys
from operator import add
from math import sqrt


def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
        .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
        .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))


conf = SparkConf() \
    .setAppName("MovieLensALS") \
    .set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

# Load and parse the data
data = sc.textFile("ratings.dat")

ratings = data.map(lambda l: l.strip().split('::')).map(lambda l: (float(l[3]) % 10, (int(l[0]), int(l[1]), float(l[2]))))


numPartitions = 4
training = ratings.filter(lambda x: x[0] < 6).values().repartition(numPartitions).cache()

validation = ratings.filter(lambda x: x[0] >= 6 and x[0] < 8).values().repartition(numPartitions).cache()

test = ratings.filter(lambda x: x[0] >= 8).values().cache()

numTraining = training.count()
numValidation = validation.count()
numTest = test.count()
print("Training: %d, validation: %d, test: %d" %
      (numTraining, numValidation, numTest))

ranks = [8, 12]
lambdas = [0.1, 10.0]
numIters = [10, 20]
bestModel = None
bestValidationRmse = float("inf")
bestRank = 0
bestLambda = -1.0
bestNumIter = -1

for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
    model = ALS.train(training, rank, numIter, lmbda)
    validationRmse = computeRmse(model, validation, numValidation)
    print("RMSE (validation) = %f for the model trained with " % validationRmse +
          "rank = %d, lambda = %.1f, and numIter = %d." % (
              rank, lmbda, numIter))
    if (validationRmse < bestValidationRmse):
        bestModel = model
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lmbda
        bestNumIter = numIter

    testRmse = computeRmse(bestModel, test, numTest)

    print("The best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda)
          + "and numIter = %d, and its RMSE on the test set is %f." % (bestNumIter, testRmse))

sc.stop()