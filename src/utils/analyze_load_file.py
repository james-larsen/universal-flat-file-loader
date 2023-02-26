"""Analyze load file"""
#%%
import pandas as pd

#%%

df = pd.read_csv(r'Y:\Python Upload Data Files\Upload\product\Product.txt',
    sep='\t',
    dtype=str,
    encoding='UTF-16',
    na_values={'<NULL>', ''},
    #chunksize=10000
    nrows=10000
    )

#df.dtypes

df.columns = (
    df.columns.str.replace(" ", "_")
    .str.replace("(", "", regex= False)
    .str.replace(")", "", regex= False)
    .str.lower()
    )

#%%
for col in df.columns:
    print(df[col].name + '\t\t' + str(df[col].dtype))

for col in df.columns:
    print(df[col].name)
