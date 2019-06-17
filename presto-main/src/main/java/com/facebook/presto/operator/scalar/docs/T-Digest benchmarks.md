# T-Digest Benchmarks

Over the past few weeks, I have been working and experimenting with the t-digest data structure to potentially replace Q-Digest in Presto. T-Digest is made up of a collection of centroids, each associated with a mean and a weight (number of elements in the centroid), ordered by ascending mean. This digest allows you to collect specific stats from a data set using very limited space and time. One of the main uses for t-digest is the ability to calculate quantiles more efficiently. Below, you will be able to find data on different benchmarks to compare the relative performance of T-Digest to Q-Digest on varying distributions.

Unless otherwise stated, all data used was created by taking 1,000,000 samples from a normal distribution. The T-Digest structures were built using a compression factor of 100, while Q-Digest was built using 0.01 as the maximum error parameter.

### Time

One of the main advantages of t-digest is that its very fast, allowing you to build and perform operations on this data structure in a few thousand nanoseconds. Here are some of the runtimes of common digest operations:

![Digest Operations Time](https://github.com/gastonar/t-digest-docs/blob/master/time_ops.png)

As seen in the graph above, t-digest outperforms q-digest in every single operation measured. It is worth noting serialization time, which is done about 70x faster since we need to store significantly less bytes of data (more on this in below). This overall increase in performance makes sense due to the underlying data structures used to represent each digest. Q-digest uses a tree, so we need to traverse through it for every operation. Meanwhile, t-digest simply uses a collection of centroids, which is usually smaller than the compression factor. We can perform each operation faster because the collection is indexed, allowing us to get to the specific centroid we want instead of having to iterate through all centroids. While testing the merge operation, I observed that the runtime is proportional to the compression factor, so merging thousands of digests with 1,000 or 10,000 as the compression factor can take a long time.

I also tested how the time to insert each element changes as the total number of elements being added increases. I expected it to remain somewhat constant, since the number of centroids should be relatively constant and shouldn't exceed the compression factor. The results can be seen in the graph below.

![Digest Insert Time](https://github.com/gastonar/t-digest-docs/blob/master/time_insert.png)

As expected, we can see that once we reach 10,000+ elements, the time per insertion remains constant at around 65 ns/insertion. In addition, we can see that each insertion into a t-digest is about 3x faster than inserting into q-digest, which is important when building a digest to analyze massive amounts of data.

### Storage
Another quality of t-digest is that it uses very little space to store information on the data that is added into it. This is excellent because one of the potential uses of t-digest could be saving t-digests and coming back to them in the future. Therefore, if we can optimize how many bytes of memory we need to store a digest, we would be able to store way more digests and use up less memory space. Below you will find comparisons between the number of bytes needed to serialize a t-digest compared to a q-digest for some of the most common distributions.

![Digest Space](https://github.com/gastonar/t-digest-docs/blob/master/space.png)

In the graph above, we can see the storage performance between q-digest and t-digest. Q-digest uses less space when the data is compacted rather than spread out. On the other hand, we can see how t-digest uses a similar amount of memory space regardless of the distribution or how spread out the values are. Therefore, we can see a massive difference when comparing performance over uniform distributions, where q-digest consumes over 100x more memory (graph is not scaled for that distribution). Even when the data is compact and q-digest can be stored with fewer bytes, the savings are almost negligible.

### Accuracy
While runtime and memory usage can be good benchmarks, we must test accuracy to ensure that t-digest is really worth implementing over q-digest. To do this, I used samples from the same distributions as above to test the accuracy of t-digest when retrieving quantiles. I used varying compression factors for t-digest to determine which alternative offers the best space-accuracy benefits. In addition, I tested the effects of merging multiple distributions together, which appears to have no significant impact on accuracy. Below, you will find the accuracy plots for normal and uniform distributions. The remaining distributions were tested using Java unit tests, and all of them passed with less than 1% error.

![Digest Normal Distribution](https://github.com/gastonar/t-digest-docs/blob/master/error_normal.png)

![Digest Uniform Distribution](https://github.com/gastonar/t-digest-docs/blob/master/error_uniform.png)

As seen in the graphs above, t-digest returns a better accuracy for almost all data points. In addition, we can see how accuracy of the t-digest increases as we increase the compression factor. For general purposes, it makes sense to use 100 as a standard compression factor, but if extremely high accuracy is required, it might be worth using a higher compression factor (which will take up more space in memory).

### Conclusions
Overall, I believe t-digest is definitely worth implementing into Presto. It performs better than q-digest for most data points across all benchmarks. In terms of runtime, every operation performed on t-digest takes significantly less time compared to q-digest (at least 3x faster for each metric). In addition, when distributions are sparse, t-digest offers massive storage savings, sometimes using up to 100x less bytes to store data. Finally, in terms of accuracy, t-digest once again outperforms q-digest, especially when retrieving quantiles at the tails. Therefore, I think t-digest will be an excellent addition to Presto, with promising improvements for all functions that currently use q-digest.
