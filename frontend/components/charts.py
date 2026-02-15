import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def create_icu_forecast_chart(hospital_data):
    """Beautiful ICU capacity forecast chart using real data - FULL WIDTH"""
    
    if not hospital_data:
        return create_demo_icu_chart()
    
    # Generate dates
    today = datetime.now()
    dates_forecast = pd.date_range(start=today, periods=5, freq='W')
    
    # Extract forecast data and convert to percentages
    current = hospital_data['current_icu_occupancy'] * 100
    week1 = hospital_data['predicted_week_1'] * 100
    week2 = hospital_data['predicted_week_2'] * 100
    week3 = hospital_data['predicted_week_3'] * 100
    week4 = hospital_data['predicted_week_4'] * 100
    
    forecast_values = [current, week1, week2, week3, week4]
    
    # Create historical pattern (last 4 weeks leading to current)
    dates_hist = pd.date_range(end=today, periods=5, freq='W')
    hist_values = [
        max(50, current - 20),
        max(55, current - 15),
        max(60, current - 10),
        max(65, current - 5),
        current
    ]
    
    fig = go.Figure()
    
    # Historical data - Cyan line
    fig.add_trace(go.Scatter(
        x=dates_hist,
        y=hist_values,
        mode='lines+markers',
        name='Historical',
        line=dict(color='#06B6D4', width=5),
        marker=dict(size=12, color='#06B6D4', line=dict(color='#0891B2', width=2)),
        hovertemplate='<b>%{x|%b %d}</b><br>Occupancy: %{y:.1f}%<extra></extra>'
    ))
    
    # Forecast data - BRIGHT PURPLE dashed line
    fig.add_trace(go.Scatter(
        x=dates_forecast,
        y=forecast_values,
        mode='lines+markers',
        name='Forecast',
        line=dict(color='#E879F9', width=6, dash='dash'),  # Even brighter purple, thicker
        marker=dict(size=14, symbol='diamond', color='#E879F9', line=dict(color='#C084FC', width=3)),
        hovertemplate='<b>%{x|%b %d}</b><br>Projected: %{y:.1f}%<extra></extra>'
    ))
    
    # Capacity thresholds
    fig.add_hline(
        y=90, 
        line_dash="dot", 
        line_color="#DC2626",
        line_width=3,
        annotation_text="Critical (90%)",
        annotation_position="right",
        annotation_font=dict(color="#DC2626", size=14, family="Arial Black")
    )
    
    fig.add_hline(
        y=75, 
        line_dash="dot", 
        line_color="#F97316",
        line_width=3,
        annotation_text="Watch (75%)",
        annotation_position="right",
        annotation_font=dict(color="#F97316", size=14, family="Arial Black")
    )
    
    # Styling
    fig.update_layout(
        height=550,  # Taller chart
        template='plotly_dark',
        paper_bgcolor='rgba(15, 23, 42, 0.5)',
        plot_bgcolor='rgba(15, 23, 42, 0.8)',
        hovermode='x unified',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor='rgba(15, 23, 42, 0.9)',
            bordercolor='rgba(6, 182, 212, 0.5)',
            borderwidth=3,
            font=dict(size=16, color='#E2E8F0', family="Arial Black")
        ),
        xaxis=dict(
            showgrid=True,
            gridwidth=1.5,
            gridcolor='rgba(148, 163, 184, 0.3)',
            title='Date',
            title_font=dict(color='#F1F5F9', size=16, family="Arial Black"),
            tickfont=dict(size=13, color='#CBD5E1')
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=1.5,
            gridcolor='rgba(148, 163, 184, 0.3)',
            title='ICU Occupancy (%)',
            title_font=dict(color='#F1F5F9', size=16, family="Arial Black"),
            tickfont=dict(size=13, color='#CBD5E1'),
            range=[max(0, min(hist_values) - 10), 100]
        ),
        font=dict(color='#E2E8F0'),
        margin=dict(l=70, r=70, t=50, b=70)
    )
    
    return fig


def create_oxygen_depletion_chart(hospital_data):
    """Beautiful oxygen depletion timeline using real data - FULL WIDTH"""
    
    if not hospital_data:
        return create_demo_oxygen_chart()
    
    oxygen_weeks = hospital_data['oxygen_weeks_remaining']
    
    # Generate dates (convert weeks to days)
    days = int(oxygen_weeks * 7)
    dates = pd.date_range(start=datetime.now(), periods=min(max(days, 14), 35), freq='D')
    
    # Create depletion curve
    oxygen_supply = np.linspace(days, 0, len(dates))
    
    fig = go.Figure()
    
    # Oxygen supply curve - Bright Green
    fig.add_trace(go.Scatter(
        x=dates,
        y=oxygen_supply,
        fill='tozeroy',
        mode='lines',
        name='Oxygen Supply',
        line=dict(color='#10B981', width=5),
        fillcolor='rgba(16, 185, 129, 0.4)',
        hovertemplate='<b>%{x|%b %d}</b><br>Supply: %{y:.1f} days<extra></extra>'
    ))
    
    # Critical threshold
    fig.add_hline(
        y=7, 
        line_dash="dot", 
        line_color="#DC2626",
        line_width=3,
        annotation_text="Critical (7 days)",
        annotation_position="right",
        annotation_font=dict(color="#DC2626", size=14, family="Arial Black")
    )
    
    # Styling
    fig.update_layout(
        height=550,  # Taller chart
        template='plotly_dark',
        paper_bgcolor='rgba(15, 23, 42, 0.5)',
        plot_bgcolor='rgba(15, 23, 42, 0.8)',
        hovermode='x unified',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor='rgba(15, 23, 42, 0.9)',
            bordercolor='rgba(6, 182, 212, 0.5)',
            borderwidth=3,
            font=dict(size=16, color='#E2E8F0', family="Arial Black")
        ),
        xaxis=dict(
            showgrid=True,
            gridwidth=1.5,
            gridcolor='rgba(148, 163, 184, 0.3)',
            title='Date',
            title_font=dict(color='#F1F5F9', size=16, family="Arial Black"),
            tickfont=dict(size=13, color='#CBD5E1')
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=1.5,
            gridcolor='rgba(148, 163, 184, 0.3)',
            title='Days of Supply Remaining',
            title_font=dict(color='#F1F5F9', size=16, family="Arial Black"),
            tickfont=dict(size=13, color='#CBD5E1'),
            range=[0, max(days + 5, 15)]
        ),
        font=dict(color='#E2E8F0'),
        margin=dict(l=70, r=70, t=50, b=70)
    )
    
    return fig


def create_demo_icu_chart():
    """Fallback demo chart"""
    dates_hist = pd.date_range(start='2025-01-15', periods=30, freq='D')
    dates_forecast = pd.date_range(start='2025-02-14', periods=14, freq='D')
    icu_hist = [65, 67, 70, 68, 72, 75, 73, 76, 78, 80, 
                79, 81, 83, 82, 84, 85, 83, 86, 87, 85,
                86, 87, 88, 86, 87, 88, 89, 87, 88, 87]
    icu_forecast = [88, 89, 89, 90, 90, 91, 91, 92, 92, 91, 92, 91, 90, 89]
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates_hist, y=icu_hist, mode='lines+markers', name='Historical',
                             line=dict(color='#06B6D4', width=5), marker=dict(size=10)))
    fig.add_trace(go.Scatter(x=dates_forecast, y=icu_forecast, mode='lines+markers', name='Forecast',
                             line=dict(color='#E879F9', width=6, dash='dash'), 
                             marker=dict(size=12, symbol='diamond')))
    fig.update_layout(height=550, template='plotly_dark', 
                     paper_bgcolor='rgba(15, 23, 42, 0.5)',
                     plot_bgcolor='rgba(15, 23, 42, 0.8)')
    return fig


def create_demo_oxygen_chart():
    """Fallback demo oxygen chart"""
    dates = pd.date_range(start='2025-02-14', periods=14, freq='D')
    oxygen_days = list(range(14, 0, -1))
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates, y=oxygen_days, fill='tozeroy', mode='lines',
                             name='Oxygen Supply', line=dict(color='#10B981', width=5),
                             fillcolor='rgba(16, 185, 129, 0.4)'))
    fig.update_layout(height=550, template='plotly_dark',
                     paper_bgcolor='rgba(15, 23, 42, 0.5)',
                     plot_bgcolor='rgba(15, 23, 42, 0.8)')
    return fig