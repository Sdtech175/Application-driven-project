#tantan.py
import streamlit as st 
import mysql.connector
from mysql.connector import Error
from google_auth_oauthlib.flow import Flow
from google.oauth2 import id_token
from google.auth.transport import requests
import bcrypt
import jwt
import datetime
import os
from dotenv import load_dotenv
import plotly.graph_objects as go
import pandas as pd
import random

# Load environment variables from .env
load_dotenv()

# JWT Secret Key
JWT_SECRET = os.getenv("JWT_SECRET", "your_jwt_secret_key")

# Set up Google OAuth 2.0 credentials
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'  # Only for development
CLIENT_CONFIG = {
    "web": {
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": ["http://localhost:8501/"],
    }
}

# Function to hash passwords
def hash_password(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

# Function to verify passwords
def verify_password(password, hashed):
    return bcrypt.checkpw(password.encode(), hashed.encode())

# Generate JWT token
def generate_jwt(email):
    payload = {
        'email': email,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)  # Token valid for 1 hour
    }
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')

# Decode JWT token
def decode_jwt(token):
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
    except jwt.ExpiredSignatureError:
        st.error("Session expired. Please log in again.")
        return None
    except jwt.InvalidTokenError:
        st.error("Invalid token.")
        return None

# Check if user exists in the database
def user_exists(connection, identifier):
    query = "SELECT * FROM users WHERE email = %s OR mobile = %s"
    cursor = connection.cursor(buffered=True)
    cursor.execute(query, (identifier, identifier))
    result = cursor.fetchone()
    cursor.close()
    return result

# Register a new user
def register_user(connection, full_name, email, mobile, password, referral_code):
    # Optional: Validate referral code
    if referral_code and not is_valid_referral_code(connection, referral_code):
        return False, "Invalid referral code provided."

    if user_exists(connection, email):
        return False, "User already exists"

    hashed_password = hash_password(password)
    query = """INSERT INTO users (full_name, email, mobile, password, referral_code) 
               VALUES (%s, %s, %s, %s, %s)"""
    params = (full_name, email, mobile, hashed_password, referral_code)
    if execute_query(connection, query, params):
        return True, "User registered successfully"
    return False, "Error registering user"

# Authenticate user during login with email or mobile
def authenticate_user(connection, email, mobile, password):
    query = "SELECT password FROM users WHERE email = %s OR mobile = %s"
    cursor = connection.cursor(buffered=True)
    cursor.execute(query, (email, mobile))
    result = cursor.fetchone()
    cursor.close()
    if result and verify_password(password, result[0]):
        return True
    return False

# Google OAuth for login/signup
def google_auth(auth_type="signin"):
    flow = Flow.from_client_config(
        client_config=CLIENT_CONFIG,
        scopes=["openid", "https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile"]
    )
    flow.redirect_uri = "http://localhost:8501/"
    
    if 'google_auth_state' not in st.session_state:
        authorization_url, state = flow.authorization_url(prompt="select_account")
        st.session_state.google_auth_state = state
        st.markdown(f'<a href="{authorization_url}" target="_self" class="google-auth-button">{auth_type.capitalize()} with Google</a>', unsafe_allow_html=True)
    else:
        try:
            flow.fetch_token(code=st.experimental_get_query_params().get('code', [None])[0])
            credentials = flow.credentials
            id_info = id_token.verify_oauth2_token(credentials.id_token, requests.Request(), CLIENT_CONFIG['web']['client_id'])
            
            email = id_info.get('email')
            full_name = id_info.get('name')
            
            connection = create_db_connection()
            if connection:
                if not user_exists(connection, email):
                    register_user(connection, full_name, email, "", "google_auth", "")
                
                st.session_state.jwt_token = generate_jwt(email)
                st.success(f"{auth_type.capitalize()} with Google successful")
                connection.close()
                return True
        except Exception as e:
            st.error(f"Error during Google authentication: {e}")
            del st.session_state.google_auth_state
    return False

# MySQL Database connection
def create_db_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")  # Make sure DB_NAME is set properly
        )
        if connection.is_connected():
            return connection
    except Error as e:
        st.error(f"Error connecting to MySQL Database: {e}")
    return None

# Execute SQL queries
def execute_query(connection, query, params=None):
    cursor = connection.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        connection.commit()
        return True
    except Error as e:
        st.error(f"Error executing query: {e}")
        return False
    finally:
        cursor.close()

# Check if referral code is valid (returns True if it exists)
def is_valid_referral_code(connection, referral_code):
    query = "SELECT * FROM users WHERE referral_code = %s"
    cursor = connection.cursor(buffered=True)
    cursor.execute(query, (referral_code,))
    result = cursor.fetchone()
    cursor.close()
    return result is not None  # Returns True if a valid referral code is found

# Simulated stock data function
def get_simulated_stock_data(symbol):
    dates = pd.date_range(end=pd.Timestamp.now(), periods=100, freq='1min')
    data = pd.DataFrame(index=dates)
    data['Open'] = [random.uniform(100, 200) for _ in range(len(dates))]
    data['High'] = data['Open'] + [random.uniform(0, 5) for _ in range(len(dates))]
    data['Low'] = data['Open'] - [random.uniform(0, 5) for _ in range(len(dates))]
    data['Close'] = [random.uniform(low, high) for low, high in zip(data['Low'], data['High'])]
    return data

# Plot stock data
def plot_stock_data(data, symbol):
    fig = go.Figure(data=[go.Candlestick(x=data.index,
                open=data['Open'],
                high=data['High'],
                low=data['Low'],
                close=data['Close'])])
    fig.update_layout(title=f"{symbol} Stock Price (Simulated)", xaxis_title="Time", yaxis_title="Price")
    return fig

# Display stock dashboard
def show_stock_dashboard():
    st.subheader("Stock Market Dashboard (Simulated Data)")
    symbol = st.text_input("Enter stock symbol (e.g., AAPL, GOOGL)", "AAPL")
    data = get_simulated_stock_data(symbol)
    
    st.plotly_chart(plot_stock_data(data, symbol))
    
    current_price = data['Close'].iloc[-1]
    daily_change = ((current_price - data['Open'].iloc[0]) / data['Open'].iloc[0]) * 100
    
    col1, col2 = st.columns(2)
    col1.metric("Current Price", f"${current_price:.2f}")
    col2.metric("Daily Change", f"{daily_change:.2f}%", f"{daily_change:.2f}%")
    
    st.subheader("Recent Trades (Simulated)")
    st.dataframe(data.tail())

# Main function to run the Streamlit app
def main():
    # Custom CSS for improved UI
    st.markdown("""
    <style>
        body {
            background-color: #E0E0E0;  /* Light gray background */
            color: #333333;
            font-family: 'Arial', sans-serif;
        }
        .header {
            text-align: center;
            color: #1E90FF;  /* Primary Color */
            font-family: 'Georgia', serif;  /* Different font for the heading */
            font-size: 2.5rem;
            margin: 30px 0;
        }
        .card {
            background-color: #FFFFFF;  /* White background for forms */
            border-radius: 10px;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
            padding: 20px;
            margin: 20px auto;
            width: 90%;                  /* Responsive width */
            max-width: 400px;           /* Limit max width */
        }
        .submit-button {
            background-color: #1E90FF;  /* Primary color */
            color: white;
            padding: 10px;
            border-radius: 5px;
            border: none;
            width: 100%;                /* Full width */
            cursor: pointer;
            font-size: 16px;
            margin-top: 10px;
        }
        .submit-button:hover {
            background-color: #48B5FF;  /* Slightly lighter blue */
        }
        .google-auth-button {
            color: white;
            background-color: #4285F4;  /* Google color */
            padding: 10px;
            border-radius: 5px;
            text-align: center;
            display: block;
            margin: 20px auto;
            text-decoration: none;
            width: 100%;
            font-size: 16px;
        }
        .google-auth-button:hover {
            background-color: #357AE8;
        }
        .error-text {
            color: #FF6347;             /* Error color */
        }
        .success-text {
            color: #3CB371;             /* Success color */
        }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("<h1 class='header'>Welcome to Stock Market App</h1>", unsafe_allow_html=True)

    # Connect to the database
    connection = create_db_connection()
    
    # JWT-based session management
    if 'jwt_token' in st.session_state:
        token_data = decode_jwt(st.session_state.jwt_token)
        if token_data:
            st.success(f"Welcome {token_data['email']}!", icon="✅")
            show_stock_dashboard()
            if st.button("Logout"):
                st.session_state.pop('jwt_token')
                st.experimental_rerun()
            return
    
    # Tabs for Login and Signup
    tab1, tab2 = st.tabs(['Login', 'Signup'])

    # Login tab
    with tab1:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown("<h2 style='text-align: center; color: #1E90FF;'>Login</h2>", unsafe_allow_html=True)
        with st.form('login_form'):
            email = st.text_input('Email', placeholder='Enter Your Email...', max_chars=50)
            mobile = st.text_input('Mobile Number', placeholder='Enter Your Mobile Number...', max_chars=15)
            password = st.text_input('Password', type='password', placeholder='Enter Password...')

            submit_button = st.form_submit_button('Login', on_click=lambda: st.session_state.pop('error_message', None))

            if submit_button:
                if authenticate_user(connection, email, mobile, password):
                    st.session_state.jwt_token = generate_jwt(email)
                    st.success('Login successful', icon="✅")
                    st.experimental_rerun()
                else:
                    st.error('Invalid email/mobile number or password', icon="❌")

        # Google Sign-In
        if st.button("Login with Google"):
            google_auth("signin")
        st.markdown("</div>", unsafe_allow_html=True)

    # Signup tab
    with tab2:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown("<h2 style='text-align: center; color: #1E90FF;'>Signup</h2>", unsafe_allow_html=True)
        with st.form('signup_form'):
            full_name = st.text_input('Full Name', placeholder='Enter Your Full Name...')
            email = st.text_input('Email Address', placeholder='Enter Your Email...')
            mobile = st.text_input('Mobile Number', placeholder='Enter Your Mobile Number...')
            password = st.text_input('Password', type='password', placeholder='Create a Password...')
            referral_code = st.text_input('Referral Code (Optional)', placeholder='Enter Referral Code...')
            submit_button = st.form_submit_button('Signup')

            if submit_button:
                status, message = register_user(connection, full_name, email, mobile, password, referral_code)
                if status:
                    st.success(message, icon="✅")
                else:
                    st.error(message, icon="❌")

            # Terms and Conditions agreement
            terms_and_conditions = st.checkbox("You agree to the Terms and Conditions and Privacy Policy.", value=False)
            if not terms_and_conditions:
                st.warning("You must agree to the Terms and Conditions and Privacy Policy to sign up.", icon="⚠️")

            st.markdown("Terms and Conditions: [Terms and Conditions](https://marketsentimentsdecoder.com/terms-and-conditions/) | Privacy Policy: [Privacy Policy](https://marketsentimentsdecoder.com/privacy-policy/)")

        # Google Sign-Up
        if st.button("Sign up with Google"):
            google_auth("signup")
        st.markdown("</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()