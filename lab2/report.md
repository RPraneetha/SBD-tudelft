# Lab 2

For this assignment, we have chosen to run our DataFrame Implementation from Lab 1 on AWS as it scales better than the RDD implementation for large sets of data. We had previously defined our metric as the total cost of computation, as it will directly influence how often such a computation could be run in comparison to the value created by the result of the computation. The total cost of computation therefore is dependent on what type of machines we choose (slower machines being much cheaper than the faster ones) and how much time the machines take to process our data, as machines can be charged for the amount of time they’re in use. 
The total cost of computation can hence be optimised by finding a balance between the cost of the machines and the time taken for the implementation to run on them.

## Implementation

To run our analysis in AWS we packaged our application into a JAR file using ```sbt package```. We uploaded this file to an S3 bucket, and created a step to run it in an EMS cluster. By making the step type ‘Spark application’ and then specifying the path of our JAR file, as well as the class including the main method, we were able to run our application once the cluster had started up. Unfortunately, the first time we ran our program on AWS, it did not work due to some configuration issues. We quickly found the culprit to be the following line in our implementation:
```scala
val spark = SparkSession.builder.config("spark.master", "local").getOrCreate()
```
As soon as we removed ```.config("spark.master", "local")```, it was able to run on AWS immediately.

To let us easily adjust the number of segments we wanted to test our analysis on, we used program parameters to specify the file path. Initially, we tried running with just a single file (e.g. ```s3://gdelt-open-data/v2/gkg/20150218230000.gkg.csv```). On the slower cores, this ran in about a minute. Of course much of this minute is setup time and overhead, so this result is not very telling about the actual performance. Unfortunately we were unable to consistently locate the result of this analysis in the log files. To combat this, we introduced another parameter to specify the output path. This let us specify exactly where we wanted the output to be written to. We could get this to work very easily using the line below:
```scala
result.write.format("json").json(args(1))
```
For the output location, we simply specified a path within our own bucket, such as ```s3://group38/output/```. 

Once we got all of this working correctly, we started to increase the amount of data we were running our analysis on. We changed the path to ```s3://gdelt-open-data/v2/gkg/201502*.gkg.csv``` which includes a wildcard, resulting in just under 1000 segments. This took about 9 minutes on the slow cores, but the program ran successfully. At this point we decided to try running our application on a larger number of faster cores. On these, the same path with about 1000 segments ran in less than a minute. 

We then modified our path again to ```s3://gdelt-open-data/v2/gkg/2015*.gkg.csv```, resulting in all the data from 2015 and a much greater number of segments. This ran in 2 minutes, still easily fast enough. However, when we tried including every single segment (the full dataset), we ran into more trouble. Our application quickly ran out of memory and the analysis did not complete. After some research we realised that this was because we were using the poorly fitting default configuration. To fix this, we could pass options to our cluster or to individual steps. While we first tried organising this for the entire cluster, we found this to be rather cumbersome as we would have to terminate, clone and restart the cluster each time we wanted to adjust these values. As this could take easily over 20 minutes per change, we switched to specifying it per step instead.

By looking at the specifications of our cluster we came up with some sensible values for the assigned memory, number of executors and the number of cores per executor. As we have 20 cores (excluding the master), we decided to set the number of executors to 20. Each core then has 36 CPU threads and 60 GiB of RAM. Therefore we set the number of cores per executor to 36, and the amount of RAM per executor to 50 GB to leave some room for the OS and overhead. This turned out to be too great a number as the maximum allowed by Spark was just under 54 GB, but this included the driver memory. We therefore set the driver memory to 8 GB and the executor memory to 40 GB to allow for some addition wiggle room. The final parameters were:
```
--driver-memory 8G --executor-memory 40G --num-executors 20 --executor-cores 36
```

Once we ran the analysis with these parameters, it was able to complete the full dataset within just 5.9 minutes. The full output ended up in our bucket as specified. 

## Metrics

These are some of the metrics we could visualize through Ganglia:

![](https://user-images.githubusercontent.com/12150428/66722598-6b0d7c00-edff-11e9-8ccc-4b08e52c1154.png)
![](https://user-images.githubusercontent.com/12150428/66722602-6ba61280-edff-11e9-9c6f-b4bf271e84ad.png)
![](https://user-images.githubusercontent.com/12150428/66722601-6ba61280-edff-11e9-9059-a61c9cdd1259.png)
![](https://user-images.githubusercontent.com/12150428/66722600-6ba61280-edff-11e9-8491-a683e5e1d3f3.png)
![](https://user-images.githubusercontent.com/12150428/66722599-6b0d7c00-edff-11e9-9498-eff0f9a598b2.png)

By analyzing these graphs, we can see that there are no evident bottlenecks in the clusters. As could be expected, the CPU load increases during the initial setup of the cluster, and plateaus during the processing of the dataset and the consequent actions performed on it, and sharply decreases after it has completed all the operations.


