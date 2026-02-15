import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import streamlit as st

# Load environment variables
load_dotenv()

@st.cache_resource
def get_db_connection():
    """Create and cache database connection"""
    try:
        conn = psycopg2.connect(
            os.getenv('DATABASE_URL'),
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

def fetch_hospital_data():
    """Fetch data from ma_dashboard_view"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM ma_dashboard_view")
        data = cursor.fetchall()
        cursor.close()
        return data
    except Exception as e:
        st.error(f"Error fetching hospital data: {e}")
        return None

def fetch_city_summary():
    """Fetch data from ma_city_summary"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM ma_city_summary")
        data = cursor.fetchone()
        cursor.close()
        return data
    except Exception as e:
        st.error(f"Error fetching city summary: {e}")
        return None

def get_hospital_by_name(hospital_name):
    """Get specific hospital data by name"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM ma_dashboard_view WHERE hospital_name = %s",
            (hospital_name,)
        )
        data = cursor.fetchone()
        cursor.close()
        return data
    except Exception as e:
        st.error(f"Error fetching hospital: {e}")
        return None

def get_all_hospital_names():
    """Get list of all hospital names"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT hospital_name FROM ma_dashboard_view ORDER BY hospital_name")
        data = cursor.fetchall()
        cursor.close()
        return [row['hospital_name'] for row in data]
    except Exception as e:
        st.error(f"Error fetching hospital names: {e}")
        return []