# parses crwd files
import sys
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

# get the start and end dates of the dataframes
# to use for naming the output csv file
bf_start = bf.index.min()
bf_end = bf.index.max()
st_start = st.index.min()
st_end = st.index.max()

# build the filenames
bf_filename = f"baseflow_{bf_start}-{bf_end}_{file_name}"
st_filename = f"storm_{st_start}-{st_end}_{file_name}"

# and finally write files out to csv
bf.to_csv(bf_filename, index=True)
st.to_csv(st_filename, index=True)