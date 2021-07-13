# Dependencies

````
    sbt >= 1.4.7
````

Should be available by default on the IC Cluster. Otherwise, refer to each project installation instructions.

# Datasets

## ml-100k

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

## ml-1m

Download the ````ml-1m.zip```` dataset in the ````data/```` folder:
````
> mkdir -p data
> cd data
> wget http://files.grouplens.org/datasets/movielens/ml-1m.zip
````

Check the integrity of the file with (it should give the same number as below):
````
> md5 -q ml-1m.zip
c4d9eecfca2ab87c1945afe126590906
````

Unzip:
````
> unzip ml-1m.zip
````

Download the ````ml-10m.zip```` dataset in the ````data/```` folder:
````
> mkdir -p data
> cd data
> wget http://files.grouplens.org/datasets/movielens/ml-10m.zip
````

Unzip:
````
> unzip ml-10m.zip
````

Copy the scripts for splitting ratings from ````data/ml-10M100K/```` to
````data/ml-1m````:
````
> cp data/ml-10M100K/allbut.pl data/ml-10M100k/split_ratings.sh data/ml-1m
````

Split ratings:
````
> cd data/ml-1m
> ./split_ratings.sh
````

After a few seconds, you should obtain the ````data/ml-1m/ra.train```` and
````data/ml-1m/ra.test````.

# Usage

## Compute optimized k-NN predictions (without Spark)

````
> cd optimizing
> sbt "run --train ../data/ml-100k/u1.base --test ../data/ml-100k/u1.test --k 100 --json optimizing.json --users 943 --movies 1682 --separator \"\t\""
````

## Compute scaled k-NN predictions (with Spark)

Package your application:
````
> cd scaling
> sbt clean assembly
````

Use MASTER=````yarn```` when running on the
````iccluster041.iccluster.epfl.ch```` and add the option
````--num-executors X```` to control the number of executors.
Use  MASTER=````"local[X]"```` (with quotes) where X is
the number of executors to use, when running locally.

### ml-100k

From directory ````scaling````:
````
spark-submit --master MASTER target/scala-2.11/m3_yourid-assembly-1.0.jar --train ../data/ml-100k/u1.base --test ../data/ml-100k/u1.test --k 200 --json scaling-100k.json --users 943 --movies 1682 --separator "\t"
````

### ml-1m

From directory ````scaling````:
````
spark-submit --master MASTER target/scala-2.11/m3_yourid-assembly-1.0.jar --train ../data/ml-1m/ra.train     --test ../data/ml-1m/ra.test --k 200 --json scaling-1m.json --users 6040 --movies 3952 --separator "::"
````

## Compute economics results
````
> cd economics
> sbt "run --json economics.json"
````

## Package for submission

Steps:

    1. Ensure you only used the dependencies listed in ````build.sbt```` in this template, and did not add any other.
    2. Remove ````project/project````, ````project/target````, and ````target/```` from each of the sub-directories ````optimizing````, ````scaling````, and ````economics````
    3. Test that all previous commands correctly produce a JSON file (after downloading/reinstalling dependencies).
    4. Remove the datasets (````data/ml-100k.zip````, ````data/ml-100k````, etc.), as well as the````project/project````, ````project/target````, and ````target/````.
    5. Add your report and any other necessary files listed in the Milestone description (see ````Deliverables````).
    6. Zip the archive.
    7. Submit to the TA for grading.

# References

Essential sbt: https://www.scalawilliam.com/essential-sbt/

Explore Spark Interactively (supports autocompletion with tabs!): https://spark.apache.org/docs/latest/quick-start.html

Scallop Argument Parsing: https://github.com/scallop/scallop/wiki

Spark Resilient Distributed Dataset (RDD): https://spark.apache.org/docs/3.0.1/api/scala/org/apache/spark/rdd/RDD.html

JSON Serialization: https://github.com/json4s/json4s#serialization

# Credits

Erick Lavoie (Design, Implementation, Tests)

Athanasios Xygkis (Requirements, Tests)
