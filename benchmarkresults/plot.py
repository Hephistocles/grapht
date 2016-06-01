import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.ticker



plt.style.use("ggplot")
colors = {
	"Neo4J (b)": '#FFE700',
	 "PostgreSQL": '#0077bb',
	"Grapht": '#00bb33',
	"Neo4J": '#ffa500',
	"PostgreSQL-dark": '#002255',
	"Grapht-dark": '#005522',
	"Neo4J-dark": '#774500'
}

font = {'family' : 'sans-serif',
		'weight' : 'bold',
		'size'   : 22}

matplotlib.rc('font', **font)

# Plot prefetcher total DB time:
#   Salient points: 
#       For all prefetchers, number of DB calls (AKA cache misses) decreases roughly 1/size
#       For Block and Lookahead prefetchers, total DB time decreases again roughly 1/Size, but CTE lookahead increases exponentially after a point
#           TODO: Look at avg DB time per call
#           Block doesn't go as low as lowest lookahead
#       Total runtime pretty much follows total DB time
#           Min lookahead time appears to be ever so slightly faster than CTE. But benefit of lookahead really that it is roughly stable at large
#           values, whereas for CTE we would need to get exactly the optimal point to benefit.
#       Variance slightly higher for Lookahead, presumably as random deviation from DB calls stack up
			
		
# df = pd.read_csv("../benches/prefetcher-bylength.tsv", sep='\t')
# for prefetcher, d in df.groupby("Prefetcher"):
# 	# skip the null prefetcher since it has no size
# 	# TODO: plot as a baseline on other graphs
# 	# TODO: fix y-axis
#     if (prefetcher == "Null Prefetcher"):
#         continue
#     fig, ax = plt.subplots()
#     ax.set_yscale("log", nonposy='clip')
#     for endpoint, d in d.groupby("Hop"):
#         vals = d.groupby("Size")["Total Runtime"]
#         means = vals.mean()/1e9
#         errors = vals.std()/1e9
#         means.plot(yerr=errors, label=endpoint)
#     plt.legend(loc="upper right")
#     plt.suptitle(prefetcher)


# # Plot prefetcher total DB time:
# #     Salient points: 
# #         For all prefetchers, number of DB calls decreases roughly 1/size
# #             DB calls = Cache Misses for block and CTE, but not for Itr since we make several calls for each miss
# #             For Itr, DB calls decreases as for Lookahead until we hit zero miss rate, when DB calls increases linearly with depth
# #         For Block and Lookahead prefetchers, total DB time decreases again roughly 1/Size, but CTE lookahead increases exponentially after a point
# #             TODO: Look at avg DB time per call
# #             Block doesn't go as low as lowest lookahead
# #         Total runtime pretty much follows total DB time
# #             Min lookahead time appears to be ever so slightly faster than CTE. But benefit of lookahead really that it is roughly stable at large
# #             values, whereas for CTE we would need to get exactly the optimal point to benefit.
# #         Variance slightly higher for Lookahead, presumably as random deviation from DB calls stack up
			
		
df = pd.read_csv("../benches/prefetcher-10.tsv",sep='\t')
for prefetcher, d in df.groupby("Prefetcher"):
	# skip the null prefetcher since it has no size
	# TODO: plot as a baseline on other graphs
	# TODO: fix y-axis
    if (prefetcher == "Null Prefetcher"):
        continue
    fig, ax = plt.subplots()
    ax.set_yscale("log", nonposy='clip')
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, pos: str(int(round(x))) if x>=1 else str(x)))

    for endpoint, d in d.groupby("Hops"):
        vals1 = d.groupby("Size")["Total DB time"]
        vals2 = d.groupby("Size")["Cache Misses"]
        means = vals1.mean()/(1e9*vals2.mean())
        # means = vals1.mean()
        errors = vals1.std()
        means.plot(label=endpoint)
    plt.legend(loc="lower right")
    plt.xlabel("Lookahead Depth")
    plt.ylabel("Average Cost of Cache Miss (s)")
    plt.suptitle(prefetcher)



# # Plot graph time:

# df = pd.read_csv("../benches/graph-astar-1.tsv",sep='\t')

# numberOfRoutes = 1
# ind = np.arange(numberOfRoutes)  # the x locations for the groups
# width = 0.25       # the width of the bars
# fig, ax = plt.subplots()
# fig.suptitle("A* Execution Time")
# ax.set_xscale("log", nonposy='clip')
# ax.set_xlim([1e-3,1e2])

# i = 0
# engines = ["Neo4J", "Grapht", "PostgreSQL"]
# plots = []
# for length, df in df.groupby("Hops"):
#     print(length)
#     i += 1
#     g = df.groupby("Engine")
#     for engine in engines:
#     	i += 1
#     	d = g.get_group(engine)
#     	vals = d#.groupby("Endpoint")
#     	means = vals.mean()["Time"]
#     	errors = vals.std()["Time"	]
#     	plots.append(ax.barh(ind - (i*width), means, width, color=colors[engine]))
#     	# for endpoint, d in df.groupby("Endpoint"):

# # ax.yaxis.set_visible(False)
# ax.set_ylabel("Path Length")
# ax.set_xlabel("Average Execution Time (s)")
# ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, pos: str(int(round(x))) if x>=1 else str(x)))
# ax.set_yticks(- (np.arange(5) + 2.5*width))
# ax.set_yticklabels((20, 40, 60, 80, 100))
# plt.legend(plots, engines, loc="upper right")




# df = pd.read_csv("../benches/rela-leven.tsv",sep='\t')

# numberOfRoutes = 1
# ind = np.arange(numberOfRoutes)  # the x locations for the groups
# width = 0.28       # the width of the bars
# fig, ax = plt.subplots()
# fig.suptitle("Min. Levenshtein Distance Execution Time")
# ax.set_xscale("log", nonposy='clip')
# ax.set_xlim([1e-2,1e2])

# i = 0
# engines = ["Neo4J", "Grapht", "PostgreSQL"]
# g = df.groupby("Engine")
# for engine in engines:
# 	i += 1
# 	d = g.get_group(engine)
# 	vals = d#.groupby("Query")
# 	means = vals.mean()["Time"]
# 	errors = vals.std()["Time"	]
# 	ax.barh(ind - (i*0.28), means/1e9, 0.28, color=colors[engine], label=engine)
# 	# for endpoint, d in df.groupby("Endpoint"):

# ax.yaxis.set_visible(False)
# ax.set_xlabel("Execution Time (s)")
# ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, pos: str(int(round(x))) if x>=1 else str(x)))
# plt.legend(loc="lower right")



df = pd.read_csv("../benches/hybrid-astar-leven.tsv",sep='\t')

numberOfRoutes = 1
ind = np.arange(numberOfRoutes)  # the x locations for the groups
width = 0.3       # the width of the bars
fig, ax = plt.subplots()
fig.suptitle("Hybrid Query (Levenshtein + A*) Execution Time")
ax.set_xscale("log", nonposy='clip')
ax.set_xlim([1e-2,1e2])


i = 0
engines = ["Neo4J", "Grapht", "PostgreSQL"]
g = df.groupby("Engine")
for engine in engines:
	i += 1
	# if (engine == "Grapht"): continue
	d = g.get_group(engine)
	vals = d#.groupby("Route")

	means = vals.mean()["Total Time"]
	errors = vals.std()["Total Time"]
	ax.barh(ind - (i*width), means/1e9, width, color=colors[engine], label=engine)


	# relmeans=means

	# means = vals.mean()["Relational Time"]
	# errors = vals.std()["Relational Time"]
	# ax.barh(ind - (i*width), means/1e9, width, left=relmeans/1e9,  color=colors[engine], label=engine)
	# for endpoint, d in df.groupby("Endpoint"):

	ax.text(0.1, 0.166666 + (i-1)*0.333333, 
		("%.2f%% Levenshtein \n%.2f%% A*"%(100*vals.mean()["Graph Time"]/means, 100*vals.mean()["Relational Time"]/means)),
		style='italic', va='center',
        transform=ax.transAxes)

ax.yaxis.set_visible(False)
ax.set_xlabel("Execution Time (s)")
ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, pos: str(int(round(x))) if x>=1 else str(x)))
plt.legend(loc="upper right")
# plt.show()



# df = pd.read_csv("../benches/hybrid-astar-leven.tsv",sep='\t')

# numberOfRoutes = 1
# ind = np.arange(numberOfRoutes)  # the x locations for the groups
# width = 0.28       # the width of the bars
# fig, ax = plt.subplots()
# fig.suptitle("Hybrid Query (Levenshtein + A*) Execution Time")
# # ax.set_xscale("log", nonposy='clip')
# # ax.set_xlim([1e-2,1e2])
# 
# ax.text(3, 8, 'boxed italics text in data coords', style='italic',
#         bbox={'facecolor':'red', 'alpha':0.5, 'pad':10})
# i = 0
# engines = ["Neo4J", "Grapht", "PostgreSQL"]
# g = df.groupby("Engine")
# for engine in engines:
# 	i += 1
# 	# if (engine == "Grapht"): continue
# 	d = g.get_group(engine)
# 	vals = d#.groupby("Route")

# 	means = vals.mean()["Graph Time"]
# 	errors = vals.std()["Graph Time"]
# 	# ax.barh(ind - (i*0.28), means/1e9, 0.28, color=colors[engine + "-dark"])


# 	relmeans=means

# 	means = vals.mean()["Relational Time"]
# 	errors = vals.std()["Relational Time"]
# 	# ax.barh(ind - (i*0.28), means/1e9, 0.28, left=relmeans/1e9,  color=colors[engine], label=engine)
# 	# for endpoint, d in df.groupby("Endpoint"):
# 	plt.figtext(ind[0] - (i*0.28), means/2e9, 'Foo', ha='center', va='bottom')
# 	print(ind[0] - (i*0.28), means/2e9)

# # ax.yaxis.set_visible(False)
# ax.set_xlabel("Execution Time (s)")
# ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, pos: str(int(round(x))) if x>=1 else str(x)))
# plt.legend(loc="upper right")






plt.show()