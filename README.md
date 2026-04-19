# Nifty50-data-driven-analytics

DS: Data-Driven Stock Analysis (Nifty50)Project OverviewDS: 
Data-Driven Stock Analysis is a comprehensive end-to-end data engineering and visualization platform designed to analyze Nifty50 market trends. 
The project focuses on transforming raw market data into actionable business intelligence using fuzzy matching for sector classification, 
SQL-backed persistence, and a multi-view interactive dashboard.This project was developed as part of a technical portfolio demonstrating proficiency in Python, Big Data concepts, and financial analytics.
Key Features
Automated ETL Pipeline: Cleans and transforms raw Nifty50 CSV data into a structured SQLite database.
Fuzzy Sector Mapping: Utilizes thefuzz library to map inconsistent stock tickers to official industry sectors with a confidence threshold (>60).
Unified Performance Metrics: Calculates monthly/yearly returns and identifies "Green vs. Red" market sentiment.
Advanced Visualization Matrix: * Diverging Bar Charts: Side-by-side comparison of Top Gainers and Losers
  .Stem Plots: Visualization of price deviations from the zero-mean
  .Risk-Reward Scatter Plots: Bi-variate analysis of volatility vs. returns.
Interactive Streamlit Dashboard: A web-based UI for real-time market summary and sector-wise drill-downs.

Technical StackLanguage: 
Python 3.14
Data Processing: Pandas, NumPy
Fuzzy Logic: thefuzz (Levenshtein Distance)
Database: SQLite3
Visualization: Matplotlib, Seaborn, Plotly
Deployment: Streamlit

📂 Project StructurePlaintext
├── Raw_file_zip.zip       # Raw Yarml files (Nifty50 historical data)
├── streamlit.py            # Main Streamlit application logic
├── ingest.py               # Data cleaning and SQL injection script
├── Sector_data.csv          # Project dependencies
└── README.md                 # Project documentation
Installation & Usage
Clone the repository:

Install dependencies:
pip install -r requirements.txt
Run the ETL process: (To initialize the SQLite DB)
Bashpython etl_process.py
Launch the Dashboard:
streamlit run Test1.py
Sample Analysis Logic
The project calculates yearly returns using the Absolute Return formula:
**Yearly Return = [(Close_last - Close_first) / Close_first] * 100**
Author Suriya Data Engineer and Science Specialist & IT Professional Specializing in Spark Optimization, Python, and Financial Data Engineering.
