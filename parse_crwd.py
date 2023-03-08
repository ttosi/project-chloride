import sys
import re
import pandas as pd

# get filename from command line args
file_name = str(sys.argv[1])

# load all data from the csv
df = pd.read_csv(file_name, parse_dates=["start", "end"])

# baseflow dataframe (bf)
bf = df[df["runoff_type"] == "Baseflow"] # filter by runoff_type
bf = bf.dropna(subset=["cl"]) # filter out rows that are NA (NaN)
# group by year and month and perform the aggregate calculations
bf = bf.groupby([df["start"].dt.year, df["start"].dt.month]).agg({
    "cl": ["count", "mean", "std", "min", "max"]
})

# storm dataframe (st)
st = df[(df["runoff_type"] == "Storm")]
st = st.dropna(subset=["cl"])
st = st.groupby([df["start"].dt.year, df["start"].dt.month]).agg({
    "cl": ["mean", "std", "min", "max"]
})

# build the filenames
prefix = re.findall("crwd-\d*", file_name)
bf_filename = f"{prefix[0]}_base.csv"
st_filename = f"{prefix[0]}_storm.csv"

# and finally write files out to csv
bf.to_csv(bf_filename, index=True)
st.to_csv(st_filename, index=True)
