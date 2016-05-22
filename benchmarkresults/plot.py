import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df = pd.read_csv("prefetcher5.csv")

plt.style.use("ggplot")

# Plot prefetcher total DB time:
# 	Salient points: 
# 		For all prefetchers, number of DB calls (AKA cache misses) decreases roughly 1/size
# 		For Block and Lookahead prefetchers, total DB time decreases again roughly 1/Size, but CTE lookahead increases exponentially after a point
# 			TODO: Look at avg DB time per call
# 		Total runtime pretty much follows total DB time
#			Min lookahead time appears to be ever so slightly faster than CTE. But benefit of lookahead really that it is roughly stable at large
#			values, whereas for CTE we would need to get exactly the optimal point to benefit.
#		Variance slightly higher for Lookahead, presumably as random deviation from DB calls stack up
# 			
# 		
for prefetcher, d in df.groupby("Prefetcher"):
	# skip the null prefetcher since it has no size
	# TODO: plot as a baseline on other graphs
	# TODO: fix y-axis
    if (prefetcher == "Null Prefetcher"):
        continue
    fig = plt.figure()
    for endpoint, d in d.groupby("End"):
        vals = d.groupby("Size")["Total Runtime"]
        means = vals.mean()
        errors = vals.std()
        means.plot(yerr=errors, label=endpoint)
    plt.legend(loc="upper right")
    plt.suptitle(prefetcher)

plt.show()
