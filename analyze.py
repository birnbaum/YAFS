import pandas as pd
import glob
import os

path = "/Users/wiesner/Dev/PureEdgeSim/PureEdgeSim/output/2019-12-19_11-07-34"
dfs = [pd.read_csv(csv) for csv in glob.glob(os.path.join(path, "*.csv"))]
df = pd.concat(dfs)

print(df)