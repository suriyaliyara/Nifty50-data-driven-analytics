import streamlit as st
import pandas as pd
import sqlite3
import numpy as np

# 1. Page Setup
st.set_page_config(page_title="Data-Driven Stock Analysis", layout="wide")
st.title(" Nifty50 Market Dashboard")

# 2. Connection
conn = sqlite3.connect('nifty50_database.db')

# 3. Define Business Cases
queries = {
    "1. Top 10 Most Volatile Stocks": "SELECT * FROM stocks",
    "2. Cumulative Return": "SELECT * FROM cumulative",
    "3. Stock Price Correlation": "SELECT * FROM correlation",
    "4. Top 5 Gainers and Losers (Month-wise)": "SELECT * FROM gainloss",
    "5. Average Yearly Return by Sector": "SELECT * FROM avg_sect_per"
}

selected_q = st.selectbox("Choose a Business Case:", list(queries.keys()))
import matplotlib.pyplot as plt
import seaborn as sns
try:
    df_result = pd.read_sql_query(queries[selected_q], conn)
    
    if not df_result.empty:
        st.subheader("Visual Analysis")
        
        # --- CHART LOGIC (Appears ABOVE Table) ---
        
        # Case 1: Volatility
        if "Volatile" in selected_q:

            st.bar_chart(df_result.set_index('Ticker')['Volatility'],color="#BA098B")
            
        # Case 2: Cumulative Return
        elif "Cumulative" in selected_q:
            st.line_chart(df_result.set_index('Ticker')['cum_ret'],color="#099A48")

        elif "Correlation" in selected_q:
            st.subheader("Nifty50 Correlation Heatmap")

            try:
        # 1. CHECK THE DATA
        # Since your debug shows Tickers as columns, your data is already pivoted.
        # We just need to make sure the 'date' index is handled or dropped.
        
        # 2. CALCULATE CORRELATION
        # This will compare ADANIENT vs ADANIPORTS etc. directly
                corr_matrix = df_result.corr()

        # 3. VISUALIZATION (Using the method that works for your project)
                fig, ax = plt.subplots(figsize=(16, 12))

                sns.heatmap(
                    corr_matrix, 
                    annot=False, 
                    cmap='coolwarm', 
                    center=0, 
                    ax=ax,
                    linewidths=0.1,
                    linecolor='white'
                        )

        # 4. STYLING
               #plt.title("Nifty50 Returns Correlation Matrix", fontsize=18)
                plt.xticks(rotation=90, fontsize=9)
                plt.yticks(fontsize=9)

        # 5. RENDER
                st.pyplot(fig)

            except Exception as e:
                st.error(f"Visualization Error: {e}")
                st.write("If you see this, check if your dataframe has non-numeric columns.")
        # Case 4: Gainers & Losers (Side-by-Side Charts)
          # Case 4: Gainers & Losers (Side-by-Side Charts)
        elif "Gainers and Losers" in selected_q:
            col1, col2 = st.columns(2)
            with col1:
                st.write("Top Gainers")
                st.bar_chart(df_result.set_index('Ticker_Gainer')['monthly_return_Gainer'],color="#078B35")
            with col2:
                st.write("Top Losers")
                st.bar_chart(df_result.set_index('Ticker_Loser')['monthly_return_Loser'],color="#9A0909")
                
        # Case 5: Average Yearly Return by Sector
        elif "Sector" in selected_q:
           # 1. Prepare colors: Green for positive, Red for negative
            colors = ['#2ecc71' if x > 0 else '#e74c3c' for x in df_result['yearly_return']]

        # 2. Create the Matplotlib Plot
            fig, ax = plt.subplots(figsize=(10, 6))
        
        # 3. Create the bars manually with our color list
            ax.bar(df_result['sector'], df_result['yearly_return'], color=colors)
        
        # 4. Cleanup the look
            plt.xticks(rotation=45, ha='right')
            ax.set_ylabel('Yearly Return (%)')
            ax.axhline(0, color='black', linewidth=0.8) # Adds a line at 0
        
        # 5. Show in Streamlit
            st.pyplot(fig)

        # --- TABLE LOGIC (Appears BELOW Chart) ---
        st.divider()
        st.subheader(f"Data Table: {selected_q}")
        st.dataframe(df_result, use_container_width=True)

    else:
        st.warning("No data found in the database for this selection.")

except Exception as e:
    st.error(f"Error: {e}")

conn.close()