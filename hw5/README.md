# Homework 5: Scalable Data Analytics and Machine Learning
### CS186, UC Berkeley, Spring 2016
#### Points: [10% of your final grade](https://sites.google.com/site/cs186spring2016/home/basic-information)
#### Due: Monday April 18, 2016; 11:59 PM
#### Note: *This homework is to be done individually!*

You will implement the K-means clustering algorithm in Spark and use it
to analyze political campaign data from the Federal Election Commission.

# Introduction

In [homework 2](https://github.com/berkeley-cs186/course/tree/master/hw2) you
got some practice writing SQL queries to ask some basic questions about a
database of startup data acquisition data.
In [homework 3](https://github.com/berkeley-cs186/course/tree/master/hw3) you
"looked under the hood" into how Spark operates, and you used Spark's RDD
operators to implement various parts of the machinery which actually constitutes
the system.

Now, you'll have a chance to put these together.
In this assignment, you'll look at data from the Federal Election Commission
(FEC), which contains details on the campaign finances of the current
candidates for the presidency of the United States.

First, you'll use Spark DataFrames to run some basic analytical queries on your
data (similar to what we did in homework 2). You'll see that Spark
is able to behave like a traditional database management system, and its API
of query operators easily supports queries handled by other SQL systems in a
scalable fashion.

While Spark evidently provides support for basic queries, the true power of its
programming model is in its flexibility supporting more complicated, iterative
tasks. As a result, you'll additionally use Spark operators to implement K-means
clustering, a widely-used algorithm in machine learning for categorizing and
classifying data.

Finally, you'll combine algorithm and data to perform some scalable machine
learning. Hopefully, you'll be able to see some trends in campaign
contributions.

# Context

You'll need to piece together the data, system, and the algorithm to make this
all work.

## The Data: Federal Election Commission Finance Data

You will be performing data analytics on about 432 MB of FEC data.
This data describes the finances of candidates running for election in 2016, and
it details contributions from individual and organizations to campaigns
disclosed to the FEC.

This data is stored as a database dump on the instructional machine servers
under the directory `~cs186/sp16/fec_2016_3_25`.
(You can also download a copy of the data we used [here]
(http://eecs.berkeley.edu/~jegonzal/fec_2016_3_25.tar.bz2)).
Each table consists of a header `.csv` file and a data `.txt` file. For example,
this is the Candidate Master header file (`cm_header_file.csv`)

```
MTE_ID,CMTE_NM,TRES_NM,CMTE_ST1,CMTE_ST2,CMTE_CITY,CMTE_ST,CMTE_ZIP,CMTE_DSGN,CMTE_TP,CMTE_PTY_AFFILIATION,CMTE_FILING_FREQ,ORG_TP,CONNECTED_ORG_NM,CAND_ID
```

and this is the first four rows of the corresponding data file (`cm.txt`)

```
C00000059|HALLMARK CARDS PAC|BROWER, ERIN MS.|2501 MCGEE|MD#288|KANSAS CITY|MO|64108|U|Q|UNK|M|C|HALLMARK CARDS, INC.|
C00000422|AMERICAN MEDICAL ASSOCIATION POLITICAL ACTION COMMITTEE|WALKER, KEVIN|25 MASSACHUSETTS AVE, NW|SUITE 600|WASHINGTON|DC|20001|B|Q||M|M|AMERICAN MEDICAL ASSOCIATION|
C00000489|D R I V E POLITICAL FUND CHAPTER 886|TOM RITTER|3528 W RENO||OKLAHOMA CITY|OK|73107|U|N||Q|L|TEAMSTERS LOCAL UNION 886|
C00000547|KANSAS MEDICAL SOCIETY POLITICAL ACTION COMMITTEE|C. RICHARD BONEBRAKE, M.D.|623 SW 10TH AVE||TOPEKA|KS|66612|U|Q|UNK|Q|T||
```

Here's a quick summary of the tables:
* [**cm**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCommitteeMaster.shtml)
contains committee information
* [**cn**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCandidateMaster.shtml)
contains candidate information
* [**ccl**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCandCmteLinkage.shtml)
contains linkage between committees and candidates
* [**itcont**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryContributionsbyIndividuals.shtml)
contains individual contributions to committees
* [**itpas2**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryContributionstoCandidates.shtml)
contains contributions between committees
* [**itoth**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCommitteetoCommittee.shtml)
links between committees

Read the specification at the
[FEC site](http://www.fec.gov/finance/disclosure/ftpdet.shtml#a2015_2016)
to further familiarize yourself with the details of these formats.

## The System: Spark SQL (DataFrames)

You'll be using [Spark
SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html#overview)s
to process the FEC data. Essentially, Spark SQL is an extension to Spark which
supports and exploits database abstractions to allow applications to manipulate
data both in the context of SQL and in the context of Spark operators.

Spark DataFrames are a specialization of the RDD which additionally organize RDD
records into columns. In fact you can always access the underlying RDD for a given
dataframe using `dataframe.rdd`.
Each DataFrame represents a SQL table, which support as
methods logical SQL operators composable with other Spark and user-defined
functions.

For instance, suppose we have a SQL table containing two-dimensional points,

```sql
CREATE TABLE points (x int, y int)
```

and we want to sample 20% of the x-values in the fourth quadrant into a
table `Samples`. In SQL we could write

```sql
CREATE TABLE samples AS
SELECT x
FROM points
WHERE x > 0
 AND y < 0
 AND rand() > 0.2;
```

Alternatively, we can read this table into a Spark DataFrame, `points`

```python
points = some_loading_function(points_data)
points.registerTempTable("points")
```

We then can compute, save, and print this sampling of points

```python
samples = points.where(points.x > 0)\
    .where(points.y < 0)\
    .sample(False, 0.2)\
    .select('x')\
    .registerTempTable("samples")

samples.show()
```

In fact, due to this duality, DataFrames support direct SQL queries as
well. To run the above query on a DataFrame, we write

```python
sql.sql("SELECT x FROM points WHERE x > 0 AND y < 0 AND rand() > 0.2")\
    .registerTempTable("samples")
```

Play around with DataFrames to get a sense of how they work. You can read the
(official
documentation)[http://spark.apache.org/docs/latest/sql-programming-guide.html#overview] for more details.

## The Algorithm: K-means Clustering

[Clustering](https://en.wikipedia.org/wiki/Cluster_analysis) is a common task
in machine learning in which similar objects, represented as vectors, are
grouped into broad categories. The [K-means clustering
algorithm](https://en.wikipedia.org/wiki/K-means_clustering) is one method
for performing this task, in which similar objects are placed into k groups
according to their distance from some designated cluster center.

Here is a sketch of the algorithm:

```python
def k_means(data, k):
    # initialization
    new_centers = initialize_centers_from_data(data)

    # main iteration
    while not_converged(...):
        old_centers = new_centers

        # initialize statistics for the iteration
        cluster_sizes = new_vector(k)
        sums = new_matrix(k, d)

        # compute statistics for new centers
        for vector in data:
            center = nearest_center(old_centers, vector)
            cluster_sizes[center] += 1.0
            sums[center] += vector

        # compute new centers from statistics
        new_centers = [s / n for (n, s) in zip(cluster_sizes, sums)]

   return new_centers
```

Notice that the algorithm's main iterative loop involves the slow improvement of
the currently-found best centers. The algorithm must thus initialize these
centers somehow before the first iteration runs. The traditional K-means
algorithm initializes its centers uniformly at random, but we can improve this
first guess by picking the first centers cleverly, using the
[K-means++ optimization](https://en.wikipedia.org/wiki/K-means%2B%2B). In
K-means++, we pick centers "far" away from previous centers to improve the
accuracy and speed of convergence of our algorithm.

### A Useful Library: NumPy

[NumPy](http://www.numpy.org/) is a Python library used for scientific
computation. It implements many useful, common functions used in numerical
applications:

```python
# a vector of length k
arr = np.zeros(k)

# a matrix of size k * d
mat = np.zeros(k, d)

# python list as NumPy vector
numbers_arr = np.asarray(python_list)

# minimum of a list
smallest = np.min(numbers_arr)

# suppose we have two lists of points, point_arr1 and point_arr2
# then get the distances (2-norm) between each pair of points
distances = np.linalg.norm(point_arr1 - point_arr2, axis=1)

# alternatively, if we want to get the distances
# between all the points in an array point_arr
# and a single point point0
distances = np.linalg.norm(point_arr - point0, axis=1)
```

It may help to use the [NumPy
documentation](http://docs.scipy.org/doc/numpy-1.10.0/reference/) as a source of
reference.

# Specification

Time to put your database skills to use!

**Note: We will not grade code that is modified outside of the cells bracketed
by _Begin Student Code Here_ and _End Student Code Here_ markers. Make sure all
the changes you want to submit lie between those markers.**

## 1. DataFrames

Your first task is to carry out basic data analytics queries against the FEC
data.

### 1a. File Format Wrangling

We must convert the raw FEC files into Spark's DataFrame format before we can
manipulate them with Spark methods. Implement `load_dataframe`, which loads a
pair of FEC files into a DataFrame.

The paths specificed by the `file_struct` objects are rooted at the location
specified by `root_path`.
We've provided a `load_header` function which reads in the `.csv` header file of
a table and returns a list of column names. Use these to properly [specify the schema](http://spark.apache.org/docs/latest/sql-programming-guide.html#programmatically-specifying-the-schema)

### 1b. Basic Analytics Queries

Now that our files have been loaded into Spark, we can use DataFrames to perform
some basic analytical queries - similar to what we can do with SQL. Observe that
we can either write raw SQL queries, or we can use DataFrame methods to directly
apply SQL operators. For this question, you may use either approach.

Answer the following questions by writing queries:
 1. Who are the 4 front-runners?
 2. How many individual contributions has each candidate received? 
    Hint: Which table might you want to join on?
 3. How much has each candidate received from individual contributions?
 4. What are the committees that are linked to each of the candidates?
    Hint: How many tables will we need for this?
 5. How many committees have contributed to each candidate?
 6. How much has each candidate received from committee contributions?

**Note: The penultimate line of each cell describes the schema for the output
we're expecting. If you change this line, make sure that your output's schema
matches these column names.**

## 2. K-means Clustering

Time to do more advanced analysis. Implement the K-means algorithm on
DataFrames.

We'll start with some toy data first before we return to the more complex
campaign data. The toy data is just a collection of 2D points; you can play
around with it to experiment.

Hint: NumPy will come in handy here for computing things like distances.

**Implementation notes**:
 * Remember, we are using DataFrames to ensure that our algorithm scales with
   the size of data input. Do not attempt to read large amounts of data into
   memory (e.g. into Python lists)! Failure to do so may cause your code to run
   _very_ slowly.

**Failure to follow the instructions above may result in point loss.**

### 2a. K-means++ Initialization

First, you will initialize the centers using the
[K-Means++
algorithm](https://en.wikipedia.org/wiki/K-means%2B%2B#Improved_initialization_algorithm).
Since we want to sample for centers in a scalable fasion, we will use
[Distributed Reservoir
Sampling](https://en.wikipedia.org/wiki/Reservoir_sampling#Distributed_Reservoir_Sampling)
to randomly select a center.

Implement `initialize_centers_plus`.
We've provided the signatures of some functions which will most likely be
helpful to you. `choose_partition_center`, `pick_between_centers`, and
`nearest_center` may be useful helper functions in your solution to
`initialize_centers_plus`.
When you've finished implementing this function, you can run it against our set
of toy data. In particular, the initial centers which are selected by K-means++
should be far away from each other.

### 2b. Main Loop

Now, using our intialization code from before, we will implement the main loop
in K-means clustering (and the rest of the function, too).

Provide an implementation of `k_means`.
We've given you `has_converged`, a useful function to determine when
the main loop of the K-means algorithm has finished converging.
In addition, you may find that filling out `compute_new_center_statistics` and
add_statistics` will help you complete your task.

You can test your implementation of the completed K-means algorithm against the
toy data. On convergence, the algorithm should produce K clusters which
accurately represent the partitioning of the data into K partitions.

## 3. Geographical Contribution Clustering

Finally, we can try out our K-means algorithm on our original set of campaign
finance data: we'll attempt to categorize campaign contributions by geographic
location using clusters.

You've just implemented the K-means algorithm, and we've already implemented
code to load the zip code data into DataFrames, so all you have to do is find
the right value of k to use. Essentially, we want a value of k which minimizes
the error from the resulting clusters. We define the error of the procedure to
be the sum of distances from points to their cluster center; at the same time, a
larger value of k carries with it the risk of overfitting.

We can get a rough idea of what value this is by trying many values and plotting
it. The "[elbow
method](https://en.wikipedia.org/wiki/Determining_the_number_of_clusters_in_a_data_set#The_Elbow_Method)"
is a good heuristic for an optimal value. Using this heuristic, what is a reasonable value of K? 
Try values of K from 2 to 30 in increments of 2.  To speed up these experiments set the convergence 
threshold for k-means to 0.001.

After determining this value, you can finally visualize campaign contributions
to different candidates as geographical clusters. Congratulations on finishing
the last project in this course!

# Testing

We've provided some toy data in the notebook to test your implementation of the
K-means algorithm. Of course, you are advised to write your own tests to catch
bugs in your implementation.

# Submission
Before you submit, remember to pull from `course` to make sure you have the most
recent version of the homework code.
To submit, remember to push to `release/hw5`:

    $ git push origin master:release/hw5

Detailed submission instructions are in [HW0](https://github.com/berkeley-cs186/course/tree/master/hw0).

We hope you all learned a lot from these projects!

 - CS 186 Staff
