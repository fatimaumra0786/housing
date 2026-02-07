import pandas as pd
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

input_file = BASE_DIR / "data" / "housing_data.csv"
df = pd.read_csv(input_file)
#df.head()
#df.columns

df = df[
    [
        'Date',
        'Region_Name',
        'Detached_Average_Price',
        'Semi_Detached_Average_Price',
        'Terraced_Average_Price',
        'Flat_Average_Price'
    ]
]
#rename columns name

df = df.rename(columns={
    'Region_Name': 'Region',
    'Detached_Average_Price': 'Detached',
    'Semi_Detached_Average_Price': 'Semi-Detached',
    'Terraced_Average_Price': 'Terraced',
    'Flat_Average_Price': 'Flat'
})

#When load a CSV, Python usually treats dates as text (strings), not real dates.first covert text to datetime object then filter after 2010 data

df['Date'] = pd.to_datetime(df['Date'])
df = df[df['Date'].dt.year >= 2010]

#melt() takes many columns and turns them into two columns:one column for the category name,one column for the value

df_long = df.melt(
    id_vars=['Date', 'Region'],
    value_vars=['Detached', 'Semi-Detached', 'Terraced', 'Flat'],
    var_name='PropertyType',
    value_name='AveragePrice'
)

#Remove Missing Prices

df_long = df_long.dropna(subset=['AveragePrice'])

#Data check, columns, missing value ,date range, sanity
#df_long.head()

#df_long.isna().sum()
#df_long = df_long.dropna(subset=['AveragePrice'])

#df_long['Date'].min(), df_long['Date'].max()

#df_long.groupby('PropertyType')['AveragePrice'].mean()

# PRICE GROWTH
# add year column

df_long['Year'] = df_long['Date'].dt.year

# yearly average price

yearly_prices = (
    df_long
    .groupby(['Region', 'PropertyType', 'Year'])
    .agg(AveragePrice=('AveragePrice', 'mean'))
    .reset_index()
)

# calculate cumulative price growth

cumulative_price_growth = (
    yearly_prices
    .sort_values('Year')
    .groupby(['Region', 'PropertyType'])
    .apply(
        lambda x: (x.iloc[-1]['AveragePrice'] - x.iloc[0]['AveragePrice'])
        / x.iloc[0]['AveragePrice'] * 100
    )
    .reset_index(name='CumulativePriceGrowthPercent')
)

# annual growth
yearly_prices['AnnualPriceGrowthPercent'] = yearly_prices.groupby(
    ['Region', 'PropertyType']
)['AveragePrice'].pct_change() * 100

final_df = yearly_prices.merge(
    cumulative_price_growth,
    on=['Region', 'PropertyType']
)

final_df = final_df.dropna(subset=['AnnualPriceGrowthPercent'])


# price_growth.head()

# merge growth back to yearly data

#final_df = pd.merge(
    #yearly_prices,
    #price_growth,
    #on=['Region', 'PropertyType'])


# create a updated csv file on computer for power bi

import os

# Create output folder
output_path = BASE_DIR / "output"
os.makedirs(output_path, exist_ok=True)

# Save file
file_path = os.path.join(output_path, "uk_housing_clean_for_powerbi.csv")
df_long.to_csv(file_path, index=False)
file_2nd = os.path.join(output_path, "house_powerbi_final.csv")
final_df.to_csv(file_2nd, index=False)

print("File saved successfully at:", file_path)



