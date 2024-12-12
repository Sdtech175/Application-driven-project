import streamlit as st     
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta

# Set the page configuration to wide layout for better space utilization
st.set_page_config(
    page_title="📈 Stock Market Insights",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for enhanced styling
st.markdown(""" 
    <style>
    /* Title Styling */
    .title {
        font-size: 36px;
        text-align: center;
        color: #000000;
        margin: 20px 0;
        font-weight: bold;
        background-color: transparent;
    }

    /* Date Selected Styling */
    .date-selected {
        font-size: 20px;
        text-align: center;
        color: #28a745; 
        font-weight: bold;
    }

    /* Subtitle Styling */
    .subtitle {
        font-size: 24px;
        text-align: center;
        color: #dc3545;
        margin: 10px 0;
        font-weight: bold;
    }

    /* Additional Insights Styling */
    .additional-insights {
        font-size: 18px;
        color: #666;
        margin: 10px 0;
    }

    /* Data Table Styling */
    .dataframe th, .dataframe td {
        font-size: 16px;
        padding: 10px;
    }

    .dataframe {
        border-collapse: collapse;
        width: 100%;
    }

    .dataframe th {
        background-color: #f5f5f5;
    }

    .dataframe td {
        border: 1px solid #ddd;
    }

    /* Sidebar Styling */
    .sidebar .sidebar-content {
        padding: 20px;
        background-color: #f8f9fa;
    }

    /* Filter Container Styling */
    .filter-container {
        margin: 10px 0;
        padding: 10px;
        background-color: #f8f9fa;
        border-radius: 5px;
    }
    </style>
""", unsafe_allow_html=True)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'rootkp',
    'database': 'stock_data_db'
}

def connect_db():
    """Establishes connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn.cursor(), conn
    except mysql.connector.Error as err:
        st.error(f"Error connecting to MySQL: {err}")
        return None, None

@st.cache_data
def get_stock_symbols():
    """Fetches distinct stock symbols from MySQL."""
    cursor, conn = connect_db()
    if cursor:
        cursor.execute("SELECT DISTINCT Symbols FROM stock_data")
        symbols = [row[0] for row in cursor.fetchall() if row[0] not in ['Date', 'Symbols']]
        cursor.close()
        conn.close()
        return symbols
    return []

@st.cache_data
def get_full_data(date):
    """Fetches all data for a specified date from MySQL."""
    cursor, conn = connect_db()
    if cursor:
        query = """
        SELECT Symbols, Multi_Months_View, Multi_Weeks_View, Weekly_View, Day_View, date, time
        FROM stock_data
        WHERE date = %s
        """
        cursor.execute(query, (date,))
        data = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        cursor.close()
        conn.close()
        # Reorder columns to have date and time at the end
        cols = [col for col in data.columns if col not in ['date', 'time']] + ['date', 'time']
        return data[~data['Symbols'].isin(['Date', 'Symbols'])][cols]
    return pd.DataFrame()

@st.cache_data
def fetch_db_data(symbols, start_date, end_date):
    """Fetches data for specific stock symbols within a date range from MySQL."""
    cursor, conn = connect_db()
    if cursor:
        placeholders = ', '.join(['%s'] * len(symbols))
        query = f"""
        SELECT Symbols, Multi_Months_View, Multi_Weeks_View, Weekly_View, Day_View, date, time
        FROM stock_data
        WHERE Symbols IN ({placeholders}) AND date BETWEEN %s AND %s
        """
        cursor.execute(query, (*symbols, start_date, end_date))
        data = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        cursor.close()
        conn.close()
        # Reorder columns to have date and time at the end
        cols = [col for col in data.columns if col not in ['date', 'time']] + ['date', 'time']
        return data[cols]
    return pd.DataFrame()

def color_coding(val):
    """Applies color coding based on stock status."""
    colors = {
        'Bullish': 'background-color: #2e8b57; color: white;',          # Sea Green
        'Bullish Sideways': 'background-color: #ffeb3b; color: black;', # Light Yellow
        'Bearish Sideways': 'background-color: #ff5722; color: white;', # Deep Orange
        'Bearish': 'background-color: #d32f2f; color: white;'           # Red
    }
    return colors.get(val, '')  # Default to no styling if value doesn't match

def prepare_data(selected_views, symbols_list, start_date, end_date):
    """Prepares and displays the stock data with color coding and insights."""
    st.sidebar.header("📊 Stock Market Insights")

    today_date = datetime.now().strftime('%Y-%m-%d')

    if not symbols_list:
        # If no symbols are selected, show today's data for all symbols
        filtered_data = get_full_data(today_date)
        date_range = f"Today ({today_date})"
    elif start_date and end_date:
        # Fetch data for selected symbols and date range
        all_data = fetch_db_data(symbols_list, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        if not all_data.empty:
            filtered_data = all_data
            date_range = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        else:
            st.warning("No data available for the selected symbols and date range.")
            return
    else:
        # If symbols are selected but no date range, show today's data for selected symbols
        filtered_data = get_full_data(today_date)
        filtered_data = filtered_data[filtered_data['Symbols'].isin(symbols_list)]
        date_range = f"Today ({today_date})"

    # Display title and date (if symbols are selected)
    st.markdown(f""" 
        {"<div class='date-selected'>🗓️ Date Range: " + date_range + "</div>" if symbols_list else "" }
        <div class="subtitle">📊 Stock Data</div>
    """, unsafe_allow_html=True)

    if not filtered_data.empty:
        # Filter columns based on selected views and ensure date/time are at the end
        base_columns = ['Symbols']
        view_columns = selected_views if selected_views else ['Multi_Months_View', 'Multi_Weeks_View', 'Weekly_View', 'Day_View']
        columns_to_display = base_columns + view_columns + ['date', 'time']

        filtered_data = filtered_data[columns_to_display]

        # Add column filters
        st.markdown("<div class='filter-container'>", unsafe_allow_html=True)
        cols = st.columns(len(view_columns))
        filter_conditions = {}
        
        status_options = ['All', 'Bullish', 'Bearish', 'Bullish Sideways', 'Bearish Sideways']
        
        for idx, col in enumerate(view_columns):
            with cols[idx]:
                st.markdown(f"**Filter {col}**")
                selected_status = st.selectbox(
                    '',
                    options=status_options,
                    key=f"filter_{col}"
                )
                if selected_status != 'All':
                    filter_conditions[col] = selected_status

        st.markdown("</div>", unsafe_allow_html=True)

        # Apply filters
        for col, condition in filter_conditions.items():
            filtered_data = filtered_data[filtered_data[col] == condition]

        # Display the filtered data with enhanced styles
        styled_df = filtered_data.style.applymap(color_coding, subset=view_columns)

        # Display the dataframe
        st.dataframe(
            styled_df,
            height=600,
            use_container_width=True
        )

        # Quick Insights section
        st.markdown(""" 
            <div class="subtitle">📋 Stock Insights</div>
            <div class="additional-insights">The data displayed includes various stock indicators for the selected date range and symbols. Use the filters to tailor the view to your needs.</div>
        """, unsafe_allow_html=True)

        # Example insights
        st.markdown("#### 📈 **Quick Insights**")
        st.write(f"- **Total Entries:** {len(filtered_data)}")
        st.write(f"- **Unique Symbols:** {filtered_data['Symbols'].nunique()}")
        st.write(f"- **Date Range:** {filtered_data['date'].min()} to {filtered_data['date'].max()}")
    else:
        st.warning("**⚠️ No data available for the selected filters.**")

# Initialize session state
if 'data_preview' not in st.session_state:
    today_date = datetime.now().strftime('%Y-%m-%d')
    st.session_state.data_preview = get_full_data(today_date)
    st.session_state.selected_date = today_date

if 'displayed_symbols' not in st.session_state:
    st.session_state.displayed_symbols = set()

# Stock Market View and Trend Analysis (Main title)
st.markdown("<h3 style='text-align: center;'>📈 Stock Market View and Trend Analysis</h3>", unsafe_allow_html=True)

# Centered stock symbol selection on the main page
st.markdown("<h3 style='text-align: center;'>🔍 Select Stock Symbols</h3>", unsafe_allow_html=True)
symbols_input = st.multiselect(
    '',
    get_stock_symbols()
)
symbols_list = [symbol.strip() for symbol in symbols_input if symbol.strip()]

# Sidebar options for date and views
start_date = st.sidebar.date_input('**📅 Start Date**', value=st.session_state.get('start_date'))
end_date = st.sidebar.date_input('**📅 End Date**', value=st.session_state.get('end_date'))
selected_views = st.sidebar.multiselect('**📊 Stock Views**', options=[
    'Multi_Months_View', 'Multi_Weeks_View', 'Weekly_View', 'Day_View'
])

# Display the filtered data
prepare_data(selected_views, symbols_list, start_date, end_date)