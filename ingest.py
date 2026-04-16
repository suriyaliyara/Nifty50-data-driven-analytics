import yaml
import glob
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import sqlite3
from thefuzz import process, fuzz

def load_yaml_file(folder_path):
    search_path = os.path.join(folder_path, r"**\*.yaml")
    #Instead of you typing slashes (like / or \), Python looks at your computer (Windows, Mac, or Linux) and adds the correct slash automatically.
    files = glob.glob(search_path,recursive=True)
    print(f"Found {len(files)} YAML files across the directory structure.")

    all_data = []
    # 2. Loop through each file path found
    for file_path in files:
        try:
            with open(file_path, 'r') as stream:
                data = yaml.safe_load(stream)
                
                # Check if the file is a dictionary (valid record)
                if isinstance(data, dict):
                    data['file_name'] = os.path.basename(file_path)
                    all_data.append(data)
            
                elif isinstance(data, list):
                # If the file is a list of records, add them all
                    for item in data:
                        if isinstance(item, dict): # Ensure each item in the list is a dict
                            item['file_name'] = os.path.basename(file_path)
                            all_data.append(item)
                    
        except yaml.YAMLError as e:
            print(f"Error parsing {file_path}: {e}")

    # 3. Create the final DataFrame
    final_df = pd.DataFrame(all_data)
    return final_df

# --- EXECUTION ---
path = r'C:\Users\Niruban\Documents\Suriya\HCLGUVI\Project_2\Raw_File'
df = load_yaml_file(path)
#print(df.head())
#print(df)#[14200 rows x 9 columns
df_cleaned= df.drop_duplicates()
#print(df_cleaned.head())#[14200 rows x 9 columns]

#print(f"Loaded {len(results)} YAML files successfully!")
#print(results[:10])

# 1. Group the data by 'Ticker'
grouped = df_cleaned.groupby('Ticker')

# 1. Create the destination folder
base_path = r"C:\Users\Niruban\Documents\Suriya\HCLGUVI\Project_2"
folder_name = "Nifty50_csv"
search_path = os.path.join(base_path, folder_name)
file_path = glob.glob(search_path)

# 2. Create the folder if it doesn't exist
if not os.path.exists(search_path):
    os.makedirs(search_path)
    print(f"Created new directory at: {search_path}")
else:
    print(f"Using Existing Directory: {search_path}")
# --- 1. Loop through each group and save ---
for ticker_name, ticker_data in grouped:
    # Use a unique name like 'current_save_path' so we don't overwrite our list
    clean_name = f"{ticker_name}_data.csv".replace(" ", "_")
    current_save_path = os.path.join(search_path, clean_name)
    
    ticker_data.to_csv(current_save_path, index=False)
    print(f"Saved: {current_save_path}")

# --- 2. Create a NEW list of all files inside the folder ---
# We use a NEW variable name 'files_to_combine' to be safe
# 1. Load the files from your folder

all_files = glob.glob(os.path.join(search_path, "*.csv"))

WIPRO_df = pd.read_csv(r'C:\Users\Niruban\Documents\Suriya\HCLGUVI\Project_2\Nifty50_csv\WIPRO_data.csv')
print(WIPRO_df.head())

# 2. Combine them
combined_df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
    
# 3. Clean hidden spaces
combined_df.columns = combined_df.columns.str.strip()
    

combined_df_Raw = combined_df.copy()
#print(" Headers aligned with your file image")
print("Actual columns now:", combined_df.columns.tolist())

    # --- 2. Analysis Block ---
    # CRITICAL FIX: We removed the .str.lower() line that was breaking 'Ticker'
    
# Check if 'Ticker' exists to avoid crash
if 'Ticker' in combined_df.columns and 'volume' in combined_df.columns:
        
    # We calculate the MEAN volume
    stack_analysis = combined_df.groupby('Ticker')['volume'].mean().reset_index()

    # 3. Sort to find the Top 10
    top_sectors = stack_analysis.sort_values(by='volume', ascending=False).head(10)

    print("\n--- TOP 10 HIGHEST VOLUME STOCKS ---")
    print(top_sectors)

        # 4. Sort to find the Top 10 lowest volume
    loss_sectors = stack_analysis.sort_values(by='volume', ascending=True).head(10)

    print("\n--- TOP 10 LOWEST VOLUME STOCKS ---")
    print(loss_sectors)
else:
        print(f"Error: Missing columns. Found: {combined_df.columns.tolist()}")

#1. Volatility Analysis:


#  CLEANING & CALCULATION
# Ensure numeric types
combined_df['close'] = pd.to_numeric(combined_df['close'], errors='coerce')

# Logic: (Close - Prev Close) / Prev Close
# We sort by date first to ensure 'shift' pulls the correct previous day
combined_df = combined_df.sort_values(['Ticker', 'date'])

#combined_df['prev_close'] = combined_df.groupby('Ticker')['close'].shift(1)
combined_df['daily_returns'] = ((combined_df['close'] - combined_df['open']) / combined_df['open']) * 100

# Drop NaNs created by shift(1) and calculation errors
clean_df = combined_df.dropna(subset=['daily_returns'])

# 2. COMPUTE VOLATILITY (Standard Deviation)
std_div = clean_df.groupby('Ticker')['daily_returns'].std().reset_index()
std_div.columns = ['Ticker', 'Volatility']

# 3. RANK TOP 10
most_volat = std_div.sort_values(by='Volatility', ascending=False).head(10)

# --- Data Sample ---
print("--- Daily Returns ---")
print(clean_df[['Ticker', 'date', 'close', 'open', 'daily_returns']].head())

#--- Standard deviation ---
print("--- Standard deviation ---")
print(std_div)

# --- Volatility Rankings ---
print("---  Top 10 Most Volatile Stocks ---")
print(most_volat)
#-----------
plt.figure(figsize=(6, 4))
plt.bar(most_volat['Ticker'], most_volat['Volatility'], color='darkorange', edgecolor='black')
plt.title('Top 10 Most Volatile Stocks (Annual Variance)', fontsize=14)
plt.ylabel('Volatility (Standard Deviation)', fontsize=12)
plt.xlabel('Stock Ticker', fontsize=12)
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
#plt.show()

#2. Cumulative Return Over Time:
#calculate the cumulative return

clean_df['cum_ret'] = clean_df.groupby('Ticker')['daily_returns'].cumsum()

final_ret = clean_df.groupby('Ticker')['cum_ret'].last().reset_index()

cum_stock = final_ret.sort_values(by='cum_ret', ascending=False).head(20)
print("--- Cumulative Returns ---")
print(cum_stock)

# ---  Line Chart ---
cum_stock = final_ret.sort_values(by='cum_ret', ascending=False).head(5)
plt.figure(figsize=(6, 4))
plt.plot(cum_stock['Ticker'], cum_stock['cum_ret'], color='darkorange')
plt.title('Top 5 Most Cumulative Returns (Annual Variance)', fontsize=14)
plt.ylabel('cum_rest (Cumulative Returns)', fontsize=12)
plt.xlabel('Stock Ticker', fontsize=12)
plt.xticks(rotation=45)
plt.grid(axis='both', linestyle='--', alpha=0.7)
plt.tight_layout()
#plt.show()  
#---4. Stock Price Correlation-----

#combined_df['close'] = pd.to_numeric(combined_df['close'], errors='coerce')
clean_df = combined_df.dropna(subset=['close'])

# PIVOT: Move Tickers to columns and Date to rows
# This creates a table where each column is a stock's daily performance
pivot_df = clean_df.pivot_table(index='date',columns='Ticker', values='daily_returns')

# CALCULATE CORRELATION
# This returns a matrix showing how every stock moves in relation to every other stock
corr_matrix = pivot_df.corr()

#--- Head Map ---
#plt.figure(figsize=(18, 15))
fig, ax = plt.subplots(figsize=(16, 12))

# 2. OPTIMIZED HEATMAP
sns.heatmap(
    corr_matrix, 
    annot=False, cmap='coolwarm', center=0, ax=ax,
    linewidths=0.1,linecolor='white') # Make the color bar a bit smaller)
# 3. LABEL ROTATION: Prevents stock names from overlapping
plt.xticks(rotation=90,fontsize=8)
plt.yticks(fontsize=8)

plt.title('Stock Correlation Matrix (Nifty50 Stock)', fontsize=10)

plt.tight_layout()
#plt.show()

#5. Top 5 Gainers and Losers (Month-wise):


# 1. Pre-processing: Ensure 'close' is numeric and sort by date
#combined_df['close'] = pd.to_numeric(combined_df['close'], errors='coerce')
combined_df['date'] = pd.to_datetime(combined_df['date'])
combined_df = combined_df.sort_values(['Ticker', 'date'])

# 2. Extract First and Last Price for each month/ticker
# .first() gets the price on the first trading day of the month
# .last() gets the price on the final trading day of the month
monthly_prices = combined_df.groupby(['Ticker', 'month'])['close'].agg(['first', 'last']).reset_index()

# 3. Calculate Monthly Return Percentage
monthly_prices['monthly_return'] = ((monthly_prices['last'] - monthly_prices['first']) / monthly_prices['first']) * 100

# 4. Clean up column names
monthly_prices.columns = ['Ticker','month', 'month_start_price', 'month_end_price', 'monthly_return']


# 1. Sort the entire dataset by month and performance
# Higher percentage at the top, lower at the bottom
sorted_data = monthly_prices.sort_values(['month', 'monthly_return'], ascending=[True, False])

# 2. Get the Winners: Keep only the first record for each month
top_gainers = sorted_data.drop_duplicates('month', keep='first').copy()
top_gainers = top_gainers[['month', 'Ticker', 'monthly_return']]

# 3. Get the Losers: Keep only the last record for each month
top_losers = sorted_data.drop_duplicates('month', keep='last').copy()
top_losers = top_losers[['month', 'Ticker', 'monthly_return']]

# 4. Merge them into a single clean dashboard-ready table
final_report = pd.merge(
    top_gainers, 
    top_losers, 
    on='month', 
    suffixes=('_Gainer', '_Loser')
)

print("--- Final Monthly Report Successfully Created ---")
print(final_report)


# 1. Setup the Frame (3 Rows, 2 Columns = 6 Slots)
# 1. Correct the flattening command (axes, not fig)


# 2. Chart 1 (Index 0): Bar Chart
# Use ax[0].bar instead of plt.bar
# 1. Prepare the combined data for the bar chart
# We take the Ticker and Return columns for both Gainers and Losers
tickers = pd.concat([final_report['Ticker_Gainer'], final_report['Ticker_Loser']])
returns = pd.concat([final_report['monthly_return_Gainer'], final_report['monthly_return_Loser']])

# 2. Plot using the combined data
# We use a color list to make Gainers Green and Losers Red
colors = ['green'] * len(final_report) + ['red'] * len(final_report)


# 1. Prepare the Data from your variables
plot_df = pd.DataFrame({
    'tickers': tickers,
    'returns': returns
})

# Create a color list: Green for positive, Red for negative
colors = ['#2ecc71' if x >= 0 else '#e74c3c' for x in plot_df['returns']]

# Create a large figure for all 5 charts (3 rows, 2 columns)
fig = plt.figure(figsize=(15, 12))
plt.subplots_adjust(hspace=0.5, wspace=0.3)

# --- CHART 1: Unified Diverging Bar Chart ---
ax1 = plt.subplot(3, 2, 1)
ax1.bar(plot_df['tickers'], plot_df['returns'], color=colors, edgecolor='black')
ax1.axhline(0, color='black', linewidth=1)
ax1.set_title("1. Top Gainers vs Losers (Unified)", fontsize=12)
plt.xticks(rotation=90)

# --- CHART 2: Stem Plot (Deviation Analysis) ---
ax2 = plt.subplot(3, 2, 2)
markerline, stemlines, baseline = ax2.stem(plot_df['tickers'], plot_df['returns'])
plt.setp(stemlines, 'color', 'gray', 'linewidth', 1)
# Use scatter to apply individual colors to markers
ax2.scatter(plot_df['tickers'], plot_df['returns'], c=colors, zorder=3, s=50)
ax2.set_title("2. Return Deviations (Stem)", fontsize=12)
plt.xticks(rotation=90)

# --- CHART 3: Horizontal Ranking ---
ax3 = plt.subplot(3, 2, 3)
ax3.barh(plot_df['tickers'], plot_df['returns'], color=colors)
ax3.invert_yaxis() 
ax3.set_title("3. Ranked Performance (Horizontal)", fontsize=12)

# --- CHART 4: Scatter Plot (Distribution) ---
ax4 = plt.subplot(3, 2, 4)
ax4.scatter(plot_df['tickers'], plot_df['returns'], c=colors, s=100, edgecolors='black')
ax4.axhline(0, color='black', alpha=0.3)
ax4.set_title("4. Performance Distribution", fontsize=12)
plt.xticks(rotation=90)

# --- CHART 5: Step Plot (Momentum) ---
ax5 = plt.subplot(3, 2, (5, 6)) 

# 1. Use a single color (like 'navy' or 'gray') for the line
ax5.step(plot_df['tickers'], plot_df['returns'], where='mid', color='navy', linewidth=2)

# 2. Add the Red/Green logic using fill_between
# This fills the area under the step chart based on positive or negative values
ax5.fill_between(plot_df['tickers'], plot_df['returns'], step="mid", 
                 where=(plot_df['returns'] >= 0), color='#2ecc71', alpha=0.3, interpolate=True)
ax5.fill_between(plot_df['tickers'], plot_df['returns'], step="mid", 
                 where=(plot_df['returns'] < 0), color='#e74c3c', alpha=0.3, interpolate=True)

ax5.set_title("5. Market Momentum (Step Chart with Fill)", fontsize=12)
ax5.axhline(0, color='black', linewidth=0.5)
plt.xticks(rotation=90)
# FINAL RENDER FOR VS CODE
print("Rendering Dashboard...")
plt.show()

sector_df = pd.read_csv(r"C:\Users\Niruban\Documents\Suriya\HCLGUVI\Project_2\Sector_data.csv")
print(sector_df.head())



# --- STEP 1: LOAD TABLES ---
# main_df: Ticker, close, date
# lookup_df: COMPANY, sector, Symbol

# --- STEP 2: CREATE THE SECTOR MAP (Fixed Logic) ---
unique_tickers = combined_df_Raw['Ticker'].unique()
lookup_choices = sector_df['Symbol'].tolist()

mapping_data = []
for raw_tick in unique_tickers:
    # We grab the result directly as a single object first to avoid unpacking errors
    result = process.extractOne(str(raw_tick), lookup_choices, scorer=fuzz.partial_ratio)
    
    if result:
        best_match = result[0]
        score = result[1]
        
        # Only map if we are confident (Score > 60)
        if score > 60:
            mapping_data.append({'Ticker': raw_tick, 'Matched_Symbol': best_match})
        else:
            mapping_data.append({'Ticker': raw_tick, 'Matched_Symbol': None})

map_tick = pd.DataFrame(mapping_data)

# --- STEP 3: ENRICH DATA ---
main_with_match = pd.merge(combined_df_Raw, map_tick, on='Ticker', how='left')

# Joining to get the 'sector' column
final_df = pd.merge(
    main_with_match, 
    sector_df[['Symbol', 'sector']], 
    left_on='Matched_Symbol', 
    right_on='Symbol', 
    how='left'
)

# --- STEP 4: CALCULATE PERFORMANCE ---
# Ensure date is sorted to get correct first/last
final_df = final_df.sort_values(['Ticker', 'date'])

stock_performance = final_df.groupby(['sector', 'Ticker'])['close'].agg(['first', 'last']).reset_index()
stock_performance['yearly_return'] = ((stock_performance['last'] - stock_performance['first']) / stock_performance['first']) * 100

sector_avg = stock_performance.groupby('sector')['yearly_return'].mean().sort_values(ascending=False).reset_index()
print(sector_avg)

print(stock_performance[stock_performance['sector'] == 'DEFENCE'].head(1))
# --- STEP 5: VISUALIZATION ---
plt.figure(figsize=(10, 6))
#sns.barplot(x='sector', y='yearly_return', data=sector_avg, palette='coolwarm')
# Updated to follow the new Seaborn v0.14.0 rules
sns.barplot(
    x='sector', 
    y='yearly_return', 
    data=sector_avg, 
    hue='sector',      # Assign the x-variable to hue
    palette='coolwarm', 
    legend=False       # Hide the legend since the x-axis already shows the sectors
)
plt.title('Average Yearly Return by Sector')
plt.ylabel('Return %')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Assuming combined_df_Raw has 'Ticker', 'close', 'open', and 'volume'

# 1. Calculate Green vs Red (Sentiment)
# We define 'Green' as Close > Open for the latest day, or use your yearly_return logic
stmt_df = stock_performance.copy()
stmt_df['Status'] = np.where(stmt_df['yearly_return'] > 0, 'Green', 'Red')

green_count = (stmt_df['Status'] == 'Green').sum()
red_count = (stmt_df['Status'] == 'Red').sum()

# 2. Calculate Average Price (Latest Close)
avg_price = combined_df_Raw['close'].mean()

# 3. Calculate Average Volume
avg_volume = combined_df_Raw['volume'].mean()

# --- PRINT THE SUMMARY ---
print("Nifty50 market summary ")
print(f"Total Stocks Analyzed : {len(stmt_df)}")
print(f"Green Stocks       : {green_count}")
print(f"Red Stocks         : {red_count}")
print(f"Average Price      : ₹{avg_price:,.2f}")
print(f"Average Volume     : {avg_volume:,.0f} shares")


# Quick Pie Chart for Sentiment
plt.figure(figsize=(6,6))
plt.pie([green_count, red_count], labels=['Green', 'Red'], colors=['#2ecc71', '#e74c3c'], autopct='%1.1f%%')
plt.title("Market summary (Green vs Red)")
plt.show()
#-----------------------sql-------------------
# 1. Create a connection to a database file
# If the file doesn't exist, SQLite will create it automatically
conn = sqlite3.connect('nifty50_database.db')
most_volat.to_sql('stocks', conn, if_exists='replace', index=False)
print("Table 'stocks' created and data inserted successfully!")

cum_stock.to_sql('cumulative', conn, if_exists='replace', index=False)
print("Table 'cumulative' created and data inserted successfully!")

corr_matrix.to_sql('correlation', conn, if_exists='replace', index=False)
print("Table 'correlation' created and data inserted successfully!")


final_report.to_sql('gainloss', conn, if_exists='replace', index=False)
print("Table 'gainloss' created and data inserted successfully!")

sector_avg.to_sql('avg_sect_per', conn, if_exists='replace', index=False)
print("Table 'avg_sect_per' created and data inserted successfully!")

conn.close()
 

