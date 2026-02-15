import streamlit as st
import time
from database.db_connection import (
    fetch_hospital_data, 
    fetch_city_summary, 
    get_hospital_by_name,
    get_all_hospital_names
)

# ============================================================================
# PAGE CONFIG - Must be first Streamlit command
# ============================================================================
st.set_page_config(
    page_title="CodeBlue",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================================================
# LOADING STATE
# ============================================================================
if 'loaded' not in st.session_state:
    with st.spinner('🏥 Initializing CodeBlue...'):
        time.sleep(1)
        st.session_state.loaded = True

# ============================================================================
# LOAD CUSTOM CSS
# ============================================================================
def load_css():
    with open('styles/custom.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

load_css()

# ============================================================================
# LOAD DATA
# ============================================================================
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_all_data():
    """Load all data from Supabase"""
    hospital_data = fetch_hospital_data()
    city_data = fetch_city_summary()
    hospital_names = get_all_hospital_names()
    return hospital_data, city_data, hospital_names

# Load data
hospital_data, city_data, hospital_names = load_all_data()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def get_alert_color(alert_level):
    """Get color for alert level"""
    alert_upper = str(alert_level).upper() if alert_level else 'NORMAL'
    if alert_upper == 'CRITICAL':
        return '#DC2626'
    elif alert_upper == 'WATCH':
        return '#F97316'
    else:
        return '#10B981'

def get_alert_emoji(alert_level):
    """Get emoji for alert level"""
    alert_upper = str(alert_level).upper() if alert_level else 'NORMAL'
    if alert_upper == 'CRITICAL':
        return '🔴'
    elif alert_upper == 'WATCH':
        return '🟡'
    else:
        return '🟢'

# ============================================================================
# HEADER WITH LOGO
# ============================================================================
st.markdown("""
<div style="display: flex; align-items: center; gap: 20px; margin-bottom: 20px;">
    <div style="background: linear-gradient(135deg, #06B6D4 0%, #0284C7 100%); 
                padding: 15px 25px; 
                border-radius: 12px; 
                box-shadow: 0 4px 15px rgba(6, 182, 212, 0.4);">
        <h1 style="color: white !important; margin: 0; font-size: 2rem; font-weight: 900;">
            🏥 CodeBlue
        </h1>
    </div>
    <div>
        <h2 style="color: #F1F5F9 !important; margin: 0; font-size: 1.5rem;">
            AI-Powered Pandemic Surge Prevention
        </h2>
        <p style="color: #94A3B8 !important; margin: 5px 0 0 0;">
            Real-time monitoring for Greater Boston healthcare systems
        </p>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# ============================================================================
# TABS NAVIGATION
# ============================================================================
tab1, tab2 = st.tabs(["🏥 Operator Dashboard", "👥 Public Portal"])

# ============================================================================
# TAB 1: OPERATOR DASHBOARD
# ============================================================================
with tab1:
    st.title("🏥 CodeBlue Operator Dashboard")
    st.markdown("Real-time hospital strain monitoring and resource allocation")
    
    # Hospital selector
    st.markdown("---")
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if hospital_names:
            selected_hospital = st.selectbox(
                "Select Hospital",
                hospital_names,
                index=0,
                key="hospital_selector"
            )
        else:
            st.error("Unable to load hospital list")
            selected_hospital = None
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Refresh Data", use_container_width=True, key="refresh_btn"):
            st.cache_data.clear()
            st.rerun()
    
    # Get selected hospital data
    if selected_hospital:
        current_hospital = get_hospital_by_name(selected_hospital)
    else:
        current_hospital = None
    
    if current_hospital:
        st.markdown("---")
        
        # ALERT BANNER with color-coded background
        alert_level = current_hospital['final_alert_level']
        
        # Normalize alert level to uppercase for comparison (handles case sensitivity)
        alert_level_upper = str(alert_level).upper() if alert_level else 'NORMAL'
        
        # Define colors and emojis based on alert level
        if alert_level_upper == 'CRITICAL':
            alert_color = '#DC2626'
            alert_bg = 'linear-gradient(135deg, #DC2626 0%, #991B1B 100%)'
            alert_emoji = '🔴'
            alert_border = '#FEE2E2'
            alert_shadow = 'rgba(220, 38, 38, 0.5)'
        elif alert_level_upper == 'WATCH':
            alert_color = '#F97316'
            alert_bg = 'linear-gradient(135deg, #F97316 0%, #EA580C 100%)'
            alert_emoji = '🟡'
            alert_border = '#FED7AA'
            alert_shadow = 'rgba(249, 115, 22, 0.5)'
        else:  # NORMAL or any other value
            alert_color = '#10B981'
            alert_bg = 'linear-gradient(135deg, #10B981 0%, #059669 100%)'
            alert_emoji = '🟢'
            alert_border = '#D1FAE5'
            alert_shadow = 'rgba(16, 185, 129, 0.5)'
        
        # Display values (convert to percentages)
        projected_peak_display = current_hospital['predicted_week_4'] * 100
        oxygen_weeks_display = current_hospital['oxygen_weeks_remaining']
        
        st.markdown(f"""
        <div style="background: {alert_bg};
                    border-radius: 12px; padding: 24px; border-left: 6px solid {alert_border};
                    box-shadow: 0 8px 30px {alert_shadow}; margin: 20px 0;">
            <div style="display: flex; align-items: center; gap: 12px;">
                <span style="font-size: 2.5rem;">{alert_emoji}</span>
                <div>
                    <h3 style="margin: 0; font-size: 1.3rem; color: white !important;">{alert_level} STATUS</h3>
                    <p style="margin: 4px 0 0 0; color: #FEE2E2 !important;">
                        {selected_hospital} ICU projected to reach {projected_peak_display:.1f}% capacity in 4 weeks. 
                        Oxygen supply: {oxygen_weeks_display:.1f} weeks remaining.
                    </p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # KEY METRICS ROW
        st.markdown("### 📊 Key Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            current_icu = current_hospital['current_icu_occupancy'] * 100  # Convert to percentage
            delta_val = current_hospital['delta_week4'] * 100  # Convert to percentage
            st.metric(
                label="Current ICU Occupancy",
                value=f"{current_icu:.1f}%",  # 1 decimal place
                delta=f"{delta_val:+.1f}%",
                delta_color="inverse"
            )
        
        with col2:
            projected_peak = current_hospital['predicted_week_4'] * 100  # Convert to percentage
            st.metric(
                label="Projected Peak (4 weeks)",
                value=f"{projected_peak:.1f}%",  # 1 decimal place
                delta="Week 4",
                delta_color="off"
            )
        
        with col3:
            oxygen_weeks = current_hospital['oxygen_weeks_remaining']
            oxygen_days = oxygen_weeks * 7
            st.metric(
                label="Oxygen Supply",
                value=f"{oxygen_days:.0f} days",  # No decimals for days
                delta=current_hospital['oxygen_alert_level'],
                delta_color="inverse" if str(current_hospital['oxygen_alert_level']).upper() == 'CRITICAL' else "normal"
            )
        
        with col4:
            strain_index = current_icu  # Already converted to percentage
            st.metric(
                label="Strain Index",
                value=f"{strain_index:.1f}/100",  # 1 decimal place
                delta=alert_level,
                delta_color="inverse" if alert_level_upper != 'NORMAL' else "normal"
            )

        st.markdown("---")
        
        # CHARTS SECTION - STACKED VERTICALLY (FULL WIDTH)
        st.markdown("### 📈 Predictive Analytics")
        
        # ICU Capacity Forecast - FULL WIDTH
        st.markdown("#### ICU Capacity Forecast")
        from components.charts import create_icu_forecast_chart
        fig_icu = create_icu_forecast_chart(current_hospital)
        st.plotly_chart(fig_icu, use_container_width=True, config={'displayModeBar': False})
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Oxygen Depletion Timeline - FULL WIDTH
        st.markdown("#### Oxygen Depletion Timeline")
        from components.charts import create_oxygen_depletion_chart
        fig_oxygen = create_oxygen_depletion_chart(current_hospital)
        st.plotly_chart(fig_oxygen, use_container_width=True, config={'displayModeBar': False})

        st.markdown("---")
        
        # RECOMMENDATIONS SECTION (if critical)
        if alert_level_upper == 'CRITICAL' and hospital_data:
            st.markdown("### 💡 Recommended Actions")
            
            # Find hospital with capacity
            receiving_hospital = None
            for h in hospital_data:
                if h['hospital_name'] != selected_hospital and h['current_icu_occupancy'] < 0.70:
                    receiving_hospital = h
                    break
            
            if receiving_hospital:
                # Calculate transfer counts
                current_capacity = current_hospital['current_icu_capacity']
                transfer_count = int((current_hospital['predicted_week_4'] - 0.75) * current_capacity)
                post_transfer = (current_hospital['predicted_week_4'] * 100) - (transfer_count / current_capacity * 100)
                impact = (current_hospital['predicted_week_4'] * 100) - post_transfer
                
                receiving_occupancy = receiving_hospital['current_icu_occupancy'] * 100
                
                st.markdown(f"""
                <div class="recommendation-card">
                    <div style="display: flex; align-items: start; gap: 16px;">
                        <div style="font-size: 2.5rem;">🚑</div>
                        <div style="flex: 1;">
                            <h4 style="margin: 0; color: #06B6D4 !important;">Transfer Recommendation</h4>
                            <p style="margin: 8px 0; color: #CBD5E1 !important;">
                                <strong>Action:</strong> Transfer {transfer_count} patients from {selected_hospital} to {receiving_hospital['hospital_name']}
                            </p>
                            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-top: 12px;">
                                <div class="stat-box">
                                    <div class="stat-label">Current Peak</div>
                                    <div class="stat-value danger">{projected_peak_display:.1f}%</div>
                                </div>
                                <div class="stat-box">
                                    <div class="stat-label">After Transfer</div>
                                    <div class="stat-value success">{post_transfer:.1f}%</div>
                                </div>
                                <div class="stat-box">
                                    <div class="stat-label">Impact</div>
                                    <div class="stat-value success">-{impact:.1f}%</div>
                                </div>
                            </div>
                            <div style="margin-top: 16px;">
                                <strong style="color: #10B981;">✓ Receiving Hospital:</strong> 
                                <span style="color: #94A3B8;">{receiving_hospital['hospital_name']} (Current: {receiving_occupancy:.1f}% capacity)</span>
                            </div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # Action buttons
                btn_col1, btn_col2, btn_col3 = st.columns([2, 2, 3])
                
                with btn_col1:
                    if st.button("✅ Approve Transfer", use_container_width=True, type="primary", key="approve_btn"):
                        st.success("Transfer approved! Notification sent to hospital administrators.")
                
                with btn_col2:
                    if st.button("📊 View Analysis", use_container_width=True, key="analysis_btn"):
                        st.info("Detailed analysis would open here...")
                
                with btn_col3:
                    if st.button("📢 Send Alert to Stakeholders", use_container_width=True, key="alert_btn"):
                        st.warning("Alerts sent to: City Emergency Management, Hospital Directors")
    else:
        st.error("Unable to load hospital data. Please check database connection.")

# ============================================================================
# TAB 2: PUBLIC PORTAL
# ============================================================================
with tab2:
    st.title("👥 Boston Healthcare System Status")
    st.markdown("Public health information and guidance")
    
    st.markdown("---")
    
    if city_data:
        # STRAIN GAUGE (CENTER PIECE)
        st.markdown("### 🏥 Healthcare System Strain Level")
        
        gauge_col1, gauge_col2, gauge_col3 = st.columns([1, 2, 1])
        
        with gauge_col2:
            # Calculate percentages
            city_icu = city_data['current_city_icu'] * 100
            alert_level = city_data['city_alert_level']
            trend = city_data['trend_direction']
            
            # Normalize alert level
            alert_level_upper = str(alert_level).upper() if alert_level else 'NORMAL'
            
            # Determine color
            if alert_level_upper == 'CRITICAL' or city_icu > 85:
                color = '#DC2626'
                level_text = 'Critical'
            elif alert_level_upper == 'WATCH' or city_icu > 70:
                color = '#F97316'
                level_text = 'Elevated'
            else:
                color = '#10B981'
                level_text = 'Normal'
            
            trend_emoji = '📈' if trend == 'RISING' else '📉' if trend == 'FALLING' else '➡️'
            
            st.markdown(f"""
            <div class="strain-gauge-container">
                <div class="strain-gauge">
                    <div class="gauge-bg">
                        <div class="gauge-fill" style="--fill-percentage: {city_icu}%;"></div>
                    </div>
                    <div class="gauge-content">
                        <div class="strain-level" style="color: {color} !important;">{level_text}</div>
                        <div class="strain-value">{city_icu:.1f}/100</div>
                    </div>
                </div>
                <div class="trend-indicator">
                    <span style="font-size: 1.5rem;">{trend_emoji}</span>
                    <span style="color: {color}; font-weight: 600;">{trend.title()} Trend (4-week)</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # ADVISORY MESSAGE
        if alert_level_upper == 'CRITICAL':
            advisory_msg = """
            **⚠️ CRITICAL ADVISORY**
            
            Healthcare demand in the Boston area is currently **critical** and showing significant strain.
            
            **What this means:**
            - Hospital capacity is severely constrained
            - Emergency rooms may have extended wait times
            - Some non-urgent procedures may be delayed
            
            **Recommended actions:**
            - Use **telehealth services** for all non-urgent consultations
            - Visit **urgent care centers** instead of ERs for minor issues
            - Postpone **non-urgent appointments** if possible
            - Keep adequate emergency supplies at home
            
            **When to seek emergency care:**
            - Severe chest pain or difficulty breathing
            - Uncontrolled bleeding
            - Sudden severe pain
            - Signs of stroke or heart attack
            """
            st.error(advisory_msg)
        elif alert_level_upper == 'WATCH':
            advisory_msg = """
            **⚠️ ADVISORY NOTICE**
            
            Healthcare demand in the Boston area is currently **elevated** and showing an upward trend.
            
            **What this means:**
            - Hospital capacity is being monitored closely
            - Some facilities may experience longer wait times
            - Healthcare system is functioning but under increased strain
            
            **Recommended actions:**
            - Use **telehealth services** for non-urgent medical consultations
            - Consider **urgent care centers** instead of emergency rooms for minor issues
            - Schedule **routine appointments** in advance when possible
            - Keep emergency supplies and medications stocked
            
            **When to seek emergency care:**
            - Severe chest pain or difficulty breathing
            - Uncontrolled bleeding
            - Sudden severe pain
            - Signs of stroke or heart attack
            """
            st.warning(advisory_msg)
        else:
            advisory_msg = """
            **ℹ️ SYSTEM STATUS**
            
            Healthcare demand in the Boston area is currently **normal** with adequate capacity.
            
            **What this means:**
            - Hospital systems are operating within normal parameters
            - Standard wait times at emergency facilities
            - All services functioning normally
            
            **General guidance:**
            - Continue routine healthcare as scheduled
            - Telehealth available for convenience
            - Emergency services ready if needed
            """
            st.info(advisory_msg)
        
        st.markdown("---")
        
        # SIMPLE CAPACITY OVERVIEW
        st.markdown("### 📊 System Capacity Overview")
        
        cap_col1, cap_col2, cap_col3 = st.columns(3)
        
        delta = city_data['delta_week4'] * 100
        
        with cap_col1:
            st.markdown(f"""
            <div class="public-metric">
                <div class="public-metric-label">Current Average</div>
                <div class="public-metric-value" style="color: {color};">{city_icu:.1f}%</div>
                <div class="public-metric-desc">Across Boston hospitals</div>
            </div>
            """, unsafe_allow_html=True)
        
        with cap_col2:
            delta_color = '#DC2626' if delta > 0 else '#10B981'
            st.markdown(f"""
            <div class="public-metric">
                <div class="public-metric-label">4-Week Projection</div>
                <div class="public-metric-value" style="color: {delta_color};">{delta:+.1f}%</div>
                <div class="public-metric-desc">Expected change</div>
            </div>
            """, unsafe_allow_html=True)
        
        with cap_col3:
            st.markdown(f"""
            <div class="public-metric">
                <div class="public-metric-label">Status</div>
                <div class="public-metric-value" style="color: {color};">{level_text}</div>
                <div class="public-metric-desc">Monitoring actively</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.error("Unable to load city summary data")
    
    st.markdown("---")
    
    # NON-EMERGENCY CARE
    st.markdown("### 🏥 Non-Emergency Care")
    
    care_col1, care_col2, care_col3 = st.columns(3)
    
    with care_col1:
        st.markdown("""
        <div style="background: rgba(6, 182, 212, 0.1); padding: 20px; border-radius: 10px; border: 2px solid rgba(6, 182, 212, 0.3); text-align: center;">
            <div style="font-size: 3rem; margin-bottom: 10px;">💻</div>
            <h4 style="color: #06B6D4 !important; margin-bottom: 10px;">Telehealth Portal</h4>
            <p style="color: #CBD5E1 !important; font-size: 0.9rem;">Connect with healthcare providers online</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🔗 Visit Portal", use_container_width=True, key="telehealth_btn"):
            st.info("🔗 Telehealth Portal: https://telehealth.example.com")
    
    with care_col2:
        st.markdown("""
        <div style="background: rgba(16, 185, 129, 0.1); padding: 20px; border-radius: 10px; border: 2px solid rgba(16, 185, 129, 0.3); text-align: center;">
            <div style="font-size: 3rem; margin-bottom: 10px;">🏥</div>
            <h4 style="color: #10B981 !important; margin-bottom: 10px;">Find Urgent Care</h4>
            <p style="color: #CBD5E1 !important; font-size: 0.9rem;">Locate nearby urgent care centers</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("📍 Locate Nearby", use_container_width=True, key="urgent_care_btn"):
            st.info("📍 Find Urgent Care: https://urgentcare.boston.gov")
    
    with care_col3:
        st.markdown("""
        <div style="background: rgba(249, 115, 22, 0.1); padding: 20px; border-radius: 10px; border: 2px solid rgba(249, 115, 22, 0.3); text-align: center;">
            <div style="font-size: 3rem; margin-bottom: 10px;">⏱️</div>
            <h4 style="color: #F97316 !important; margin-bottom: 10px;">Hospital Wait Times</h4>
            <p style="color: #CBD5E1 !important; font-size: 0.9rem;">Check current wait times at ERs</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🔍 Check Status", use_container_width=True, key="wait_times_btn"):
            st.info("🔍 Wait Times: https://waittimes.boston.gov")
    
    st.markdown("---")
    
    # EMERGENCY CONTACTS
    with st.expander("📞 Emergency Services & Important Contacts"):
        emerg_col1, emerg_col2 = st.columns(2)
        
        with emerg_col1:
            st.markdown("""
            **🚨 Emergency Services:**
            - **Emergency:** 911
            - **Poison Control:** 1-800-222-1222
            - **Mental Health Crisis:** 988
            - **Boston EMS Non-Emergency:** (617) 343-2367
            """)
        
        with emerg_col2:
            st.markdown("""
            **ℹ️ Information Lines:**
            - **Boston Public Health:** (617) 534-5395
            - **COVID-19 Info:** 211
            - **Mass General Info:** (617) 726-2000
            - **BMC Info:** (617) 638-8000
            """)

# ============================================================================
# FOOTER
# ============================================================================
st.markdown("---")
st.caption("CodeBlue © 2026 | Powered by AI | Data refreshes every 5 minutes")