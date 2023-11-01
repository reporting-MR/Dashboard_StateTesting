import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pandas_gbq
import pandas 
from google.oauth2 import service_account
from google.cloud import bigquery
import statsmodels.api as sm
from plotly.subplots import make_subplots
from prophet import Prophet

st.set_page_config(page_title="SunPower Overview Dash",page_icon="🧑‍🚀",layout="wide")

def password_protection():
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        
    if not st.session_state.authenticated:
        password = st.text_input("Enter Password:", type="password")
        correct_hashed_password = "Sunpower1234"
        
        if st.button("Login"):
            if password == correct_hashed_password:
                st.session_state.authenticated = True
                main_dashboard()
            else:
                st.error("Incorrect Password. Please try again or contact the administrator.")
    else:
        main_dashboard()

def main_dashboard():
    st.markdown("<h1 style='text-align: center; color: black;'>SunPower Overview Dash - October</h1>", unsafe_allow_html=True)
    
    # Create API client.
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = bigquery.Client(credentials=credentials)
    
    # Perform query.
    query = '''SELECT * FROM `sunpower-375201.sunpower_agg.sunpower_full_funnel` WHERE Date >= "2023-10-01" AND Date <= "2023-10-31"'''
    data = pandas.read_gbq(query, credentials=credentials)

    #Channel_Non_Truth

    # Assuming you have the following unique values lists for your filters:
    channels_unique = list(data["Channel_Non_Truth"].unique())
    types_unique = list(data["Type"].unique())
    states_unique = list(data["State_Name"].unique())
    campaigns_unique = list(data["Campaign"].unique())
    
    # Filters
    st.markdown("**Filters**")
    date_range = st.date_input('Date Range', [data['Date'].min(), data['Date'].max()])
    col02, col03, col04, col05 = st.columns(4)
    with col02:
        with st.expander("Filter Channel"):
            selected_channels = [channel for channel in channels_unique if st.checkbox(channel, value=True, key=channel)]
            if not selected_channels:  # If nothing is selected, select all
                selected_channels = channels_unique
    with col03:
        with st.expander("Filter Types"):
            selected_types = [type for type in types_unique if st.checkbox(type, value=True, key="type_" + type)]
            if not selected_types:
                selected_types = types_unique
    with col04:
        with st.expander("Filter States"):
            selected_states = [state for state in states_unique if st.checkbox(state, value=True, key=state)]
            if not selected_states:
                selected_states = states_unique    
    with col05:
        with st.expander("Filter Campaigns"):
            selected_campaigns = [campaign for campaign in campaigns_unique if st.checkbox(str(campaign), value=True, key=str(campaign))]
            if not selected_campaigns:
                selected_campaigns = campaigns_unique
    
    ##### Modify Data Based on Filters #####
    data = data[data['Channel_Non_Truth'].isin(selected_channels)]
    data = data[data['Type'].isin(selected_types)]
    data = data[data['State_Name'].isin(selected_states)]
    data = data[data['Campaign'].isin(selected_campaigns)]

    data = data[(data['Date'] >= date_range[0]) & (data['Date'] <= date_range[1])]
    
    ##### Displaying the dashboard #####
    # Collapsible data frame
    #with st.expander("Data Preview"):
    #    st.dataframe(data)
    
    #### Metrics ####
    st.markdown("<h2 style='text-align: center; color: black;'>Metrics</h2>", unsafe_allow_html=True)
    #st.subheader("Metrics")
    
    # Number of Impressions, Clicks, and Conversions
    impressions = data['Impressions'].sum()
    clicks = data['Clicks'].sum()
    conversions = data['Conversions'].sum()
    cost = data['Cost'].sum()
    leads = data['Number_of_reports__Salesforce_Reports'].sum()
    DQs = data['DQ'].sum()
    CPL = cost/leads
    data['Appts'] = pd.to_numeric(data['Appts'], errors='coerce').fillna(0).astype(int)
    Appointments = data['Appts'].sum()
    
    # Additional metrics
    ctr = clicks / impressions
    cvr = conversions / impressions
    cpc = cost / conversions
    cpa = cost / Appointments
    L2A = Appointments / leads
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write("Clicks, Impressions, and CTR")
        col11, col12, col13 = st.columns(3)
        col11.metric(label = "Total Impressions", value = '{:,}'.format(impressions))
        col12.metric(label = "Total Clicks", value = '{:,}'.format(clicks))
        col13.metric(label = "CTR", value = "{}%".format(round(ctr*100, 2)))
    
    with col2:
        st.write("Leads, DQs, and CPL")
        col21, col22, col23 = st.columns(3)
        col21.metric(label = "Leads", value = '{:,}'.format(leads))
        col22.metric(label = "DQs", value = '{:,}'.format(round(DQs)))
        col23.metric(label = "CPL", value = "{}$".format(round(CPL, 2)))
    
    with col3:
        st.write("Appts, L2A, and CPA")
        col31, col32, col33 = st.columns(3)
        col31.metric(label = "Appointments", value = '{:,}'.format(Appointments))
        col32.metric(label = "L2A", value = "{}%".format(round(L2A*100, 2)))
        col33.metric(label = "CPA", value = "{}$".format(round(cpa, 2)))
    
    ##### Line Charts Under Metrics #####
    col4, col5, col6 = st.columns(3)
    
    #Getting daily data
    
    data['Date'] = pd.to_datetime(data['Date'])
    numerical_columns = data.select_dtypes(include=['number']).columns
    
    daily_sums = data.groupby(data['Date'].dt.date)[numerical_columns].sum()
    daily_sums = daily_sums.reset_index()
    daily_sums['CTR'] = daily_sums['Clicks'] / daily_sums['Impressions']
    daily_sums['CPL'] = daily_sums['Cost'] / daily_sums['Number_of_reports__Salesforce_Reports']
    daily_sums['CPA'] = daily_sums['Cost'] / daily_sums['Appts']
    
    ####Line Chart for Clicks and CTR
    fig = go.Figure()
    # Add a line trace for daily click sums
    fig.add_trace(go.Scatter(x=daily_sums['Date'], y=daily_sums['Clicks'], mode='lines', name='Daily Clicks', yaxis='y'))
    fig.add_trace(go.Scatter(x=daily_sums['Date'], y=daily_sums['CTR'], mode='lines', name='CTR', yaxis='y2'))
    fig.update_layout(
        title='Daily Clicks and CTR',
        xaxis_title='Date',
        yaxis_title='Clicks',
        yaxis2=dict(
            title='CTR (%)',
            overlaying='y',
            side='right',
            rangemode='tozero'  # Sets the secondary y-axis to start from 0
        )
    )
    
    #### Line Chart for Leads and CPL
    fig2 = go.Figure()
    # Add a line trace for daily click sums
    fig2.add_trace(go.Scatter(x=daily_sums['Date'], y=daily_sums['Number_of_reports__Salesforce_Reports'], mode='lines', name='Daily Leads', yaxis='y', line = dict(color="Red")))
    fig2.add_trace(go.Scatter(x=daily_sums['Date'], y=daily_sums['CPL'], mode='lines', name='CPL', yaxis='y2', line=dict(color = 'orange')))
    fig2.update_layout(
        title='Daily Leads and CPL',
        xaxis_title='Date',
        yaxis_title='Leads',
        yaxis2=dict(
            title='CPL ($)',
            overlaying='y',
            side='right',
            rangemode='tozero'  # Sets the secondary y-axis to start from 0
        )
    )
    
    #### Line Chart for Appts and CPA
    fig3 = go.Figure()
    # Add a line trace for daily click sums
    fig3.add_trace(go.Scatter(x=daily_sums['Date'], y=daily_sums['Appts'], mode='lines', name='Daily Appts', yaxis='y', line = dict(color="Purple")))
    fig3.add_trace(go.Scatter(x=daily_sums['Date'], y=daily_sums['CPA'], mode='lines', name='CPA', yaxis='y2', line=dict(color = 'Green')))
    fig3.update_layout(
        title='Daily Appts and CPA',
        xaxis_title='Date',
        yaxis_title='Leads',
        yaxis2=dict(
            title='CPA ($)',
            overlaying='y',
            side='right',
            rangemode='tozero'  # Sets the secondary y-axis to start from 0
        )
    )
    
    
    with col4:
        st.plotly_chart(fig, use_container_width=True)
    
    with col5: 
        st.plotly_chart(fig2, use_container_width=True)
    
    with col6:
        st.plotly_chart(fig3, use_container_width=True)
    
    
    ### Bottom Charts ###
    bottom_left_column, bottom_right_column = st.columns(2)

    state_abbreviations = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
    'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO',
    'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
    'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
    'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
    'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'}

    # Convert full state names in your dataframe to abbreviations
    data['State_Abbreviation'] = data['State_Name'].map(state_abbreviations)

    aggregated_data = data.groupby('State_Abbreviation').agg({'Appts': 'sum'}).reset_index()
        
    with bottom_left_column:
        #Map showing leads by state
        fig_map = px.choropleth(aggregated_data, 
                        locations='State_Abbreviation', 
                        locationmode='USA-states', 
                        color='Appts', 
                        scope='usa', 
                        title='Appts by State',
                        color_continuous_scale='Viridis',
                        labels={'Appts':'Appts'})

        st.plotly_chart(fig_map, use_container_width=True)
    
    with bottom_right_column:
        # Scatter plot showing Conversions as a function of clicks with a regression line
        fig_scatter = px.scatter(data, x ='Cost', y='Conversions', trendline='ols', title='Conversions vs Cost')
        st.plotly_chart(fig_scatter, use_container_width=True)   
    
if __name__ == '__main__':
    password_protection()
