# Milestone Description

[Milestone-1.pdf](./Milestone-1.pdf)

# Dependencies

````
    sbt >= 1.4.7
````

Should be available by default on the IC Cluster. Otherwise, refer to each project installation instructions.

# Dataset

Download the ````ml-100k.zip```` dataset in the ````data/```` folder:
````
> mkdir -p data
> cd data
> wget http://files.grouplens.org/datasets/movielens/ml-100k.zip   
````

Check the integrity of the file with (it should give the same number as below):
````
> md5 -q ml-100k.zip
0e33842e24a9c977be4e0107933c0723 
````

Unzip:
````
> unzip ml-100k.zip
````

# Personal Ratings

Add your ratings in the 'data/personal.csv' file, by providing a numerical rating between [1,5] for at least 20 movies. For example, to rate the 'Toy Story' movie with '5', modify this line:

````
1,Toy Story (1995),
````

to this:
````
1,Toy Story (1995),5
````

Do include your own ratings in your final submission so we can check your answers against those provided in your report.

# Usage

## Compute statistics

````
> sbt "runMain stats.Analyzer --data data/ml-100k/u.data --json statistics.json"
````

## Compute predictions

````
> sbt "runMain predict.Predictor --train data/ml-100k/u1.base --test data/ml-100k/u1.test --json predictions.json"
````

## Compute recommendations
````
> sbt "runMain recommend.Recommender --data data/ml-100k/u.data --personal data/personal.csv --json recommendations.json"
````

## Package for submission

Steps:

    1. Update the ````name````, ````maintainer```` fields of ````build.sbt````, with the correct Milestone number, your ID, and your email.
    2. Ensure you only used the dependencies listed in ````build.sbt```` in this template, and did not add any other.
    3. Remove ````project/project````, ````project/target````, and ````target/````.  
    4. Test that all previous commands for generating statistics, predictions, and recommendations correctly produce a JSON file (after downloading/reinstalling dependencies).
    5. Remove the ml-100k dataset (````data/ml-100k.zip````, and ````data/ml-100k````), as well as the````project/project````, ````project/target````, and ````target/````. 
    6. Add your report and any other necessary files listed in the Milestone description (see ````Deliverables````).
    7. Zip the archive.
    8. Submit to the TA for grading.

# References

Essential sbt: https://www.scalawilliam.com/essential-sbt/

Explore Spark Interactively (supports autocompletion with tabs!): https://spark.apache.org/docs/latest/quick-start.html

Scallop Argument Parsing: https://github.com/scallop/scallop/wiki

Spark Resilient Distributed Dataset (RDD): https://spark.apache.org/docs/3.0.1/api/scala/org/apache/spark/rdd/RDD.html

JSON Serialization: https://github.com/json4s/json4s#serialization

# Credits

Erick Lavoie (Design, Implementation, Tests)

Athanasios Xygkis (Requirements, Tests)
