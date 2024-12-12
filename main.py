import streamlit as st
import pandas as pd
from datetime import datetime

# Set the page configuration to wide layout for better space utilization
st.set_page_config(
    page_title="📈 Stock Market Insights",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Set Pandas Styler to allow larger dataframes for styling
pd.set_option("styler.render.max_elements", 1500000)

# Custom CSS for enhanced styling, unchanged
st.markdown(""" 
    <style>
    /* Title Styling */
    .title { font-size: 36px; text-align: center; color: #000000; margin: 20px 0; font-weight: bold; }
    /* Date Selected Styling */ 
    .date-selected { font-size: 20px; text-align: center; color: #28a745; font-weight: bold; }
    /* Subtitle Styling */ 
    .subtitle { font-size: 24px; text-align: center; color: #dc3545; margin: 10px 0; font-weight: bold; }
    /* Additional Insights Styling */ 
    .additional-insights { font-size: 18px; color: #666; margin: 10px 0; }
    /* Data Table Styling */ 
    .dataframe th, .dataframe td { font-size: 16px; padding: 10px; }
    .dataframe { border-collapse: collapse; width: 100%; }
    .dataframe th { background-color: #f5f5f5; }
    .dataframe td { border: 1px solid #ddd; }
    /* Sidebar Styling */ 
    .sidebar .sidebar-content { padding: 20px; background-color: #f8f9fa; }
    /* Filter Container Styling */
    .filter-container { margin: 10px 0; padding: 10px; background-color: #f8f9fa; border-radius: 5px; }
    /* Centered Input Container */
    .center-input { display: flex; justify-content: center; align-items: center; flex-direction: column; }
    </style>
""", unsafe_allow_html=True)

# Load CSV data (replace this with your CSV file path)
CSV_FILE_PATH = r'dat/Whole data pack.csv'

@st.cache_data
def load_data_from_csv():
    
    
    try:
        data = pd.read_csv(CSV_FILE_PATH)
        data['date'] = pd.to_datetime(data['date'], format='%m/%d/%Y', errors='coerce')
        return data
    except Exception as e:
        st.error(f"Error loading CSV file: {e}")
        return pd.DataFrame()

def filter_data(data, symbols_list, start_date, end_date):
    filtered_data = data[data['Symbols'].isin(symbols_list)]
    filtered_data = filtered_data[(filtered_data['date'] >= start_date) & (filtered_data['date'] <= end_date)]
    return filtered_data

def color_coding(val):
    colors = {
        'Bullish': 'background-color: #2e8b57; color: white;',
        'Bullish Sideways': 'background-color: #ffeb3b; color: black;',
        'Bearish Sideways': 'background-color: #ff5722; color: white;',
        'Bearish': 'background-color: #d32f2f; color: white;'
    }
    return colors.get(val, '')

def apply_filters(filtered_data, view_columns, filter_conditions):
    for col in view_columns:
        if col in filter_conditions and filter_conditions[col]:
            conditions = filter_conditions[col]
            filtered_data = filtered_data[filtered_data[col].isin(conditions)]
    return filtered_data

def prepare_data(selected_views, symbols_list, start_date, end_date):
    data = load_data_from_csv()

    if symbols_list:
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)
        filtered_data = filter_data(data, symbols_list, start_date, end_date)
        date_range = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    else:
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)
        filtered_data = filter_data(data, data['Symbols'].unique(), start_date, end_date)
        date_range = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"

    st.markdown(f"<div class='date-selected'>🗓️ Date Range: {date_range}</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>📊 Stock Data</div>", unsafe_allow_html=True)

    if not filtered_data.empty:
        base_columns = ['Symbols']
        view_columns = selected_views if selected_views else ['Multi_Months_View', 'Multi_Weeks_View', 'Weekly_View', 'Day_View']
        columns_to_display = base_columns + view_columns + ['date', 'time']

        filtered_data = filtered_data[columns_to_display]

        # Sort the data by date in ascending order
        filtered_data = filtered_data.sort_values(by='date')

        st.markdown("<div class='filter-container'>", unsafe_allow_html=True)
        filter_conditions = {}
        
        status_options = ['All', 'Bullish', 'Bearish', 'Bullish Sideways', 'Bearish Sideways']
        
        for col in view_columns:
            selected_status = st.sidebar.multiselect(
                f"**Filter {col}**", options=status_options, default=['All'], key=f"filter_{col}"
            )
            if 'All' not in selected_status:
                filter_conditions[col] = selected_status

        st.markdown("</div>", unsafe_allow_html=True)

        # Apply the filters based on the selected statuses
        filtered_data = apply_filters(filtered_data, view_columns, filter_conditions)

        styled_df = filtered_data.style.applymap(color_coding, subset=view_columns)
        st.dataframe(styled_df, height=600, use_container_width=True)

        st.markdown("<div class='subtitle'>📋 Stock Insights</div>", unsafe_allow_html=True)
        st.markdown("<div class='additional-insights'>The data displayed includes various stock indicators for the selected date range and symbols. Use the filters to tailor the view to your needs.</div>", unsafe_allow_html=True)

        st.markdown("#### 📈 **Quick Insights**")
        st.write(f"- Total Entries: {len(filtered_data)}")
        st.write(f"- Unique Symbols: {filtered_data['Symbols'].nunique()}")
        st.write(f"- Date Range: {filtered_data['date'].min()} to {filtered_data['date'].max()}")
    else:
        st.warning("**⚠️ No data available for the selected filters.**")

if 'displayed_symbols' not in st.session_state:
    st.session_state.displayed_symbols = set()

st.markdown("<h3 style='text-align: center;'>📈 Stock Market View and Trend Analysis</h3>", unsafe_allow_html=True)

# Centered input for stock symbols
st.markdown("<div class='center-input'>", unsafe_allow_html=True)
st.markdown("<h3>🔍 Select Stock Symbols</h3>", unsafe_allow_html=True)

# Move the symbols input to the main area
symbols_input = st.multiselect('Choose stock symbols:', load_data_from_csv()['Symbols'].unique())
symbols_list = [symbol.strip() for symbol in symbols_input if symbol.strip()]
st.markdown("</div>", unsafe_allow_html=True)

# Current date as default for start and end dates
current_date = datetime.now().date()

# Move date inputs to the sidebar; set default to today
start_date = st.sidebar.date_input('**📅 Start Date**', value=current_date, min_value=datetime(2024, 1, 1), max_value=datetime(2024, 12, 31))
end_date = st.sidebar.date_input('**📅 End Date**', value=current_date, min_value=datetime(2024, 1, 1), max_value=datetime(2024, 12, 31))

# Ensure start date is not after end date
if start_date > end_date:
    st.error("**⚠️ Start date cannot be after end date.**")
    start_date = end_date

# Move stock views to the sidebar
selected_views = st.sidebar.multiselect('**📊 Stock Views**', ['Multi_Months_View', 'Multi_Weeks_View', 'Weekly_View', 'Day_View'])

prepare_data(selected_views, symbols_list, start_date, end_date)