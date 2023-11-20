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
from datetime import datetime, timedelta
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
    st.markdown("<h1 style='text-align: center;'>SunPower Overview Dash</h1>", unsafe_allow_html=True)
    # Calculate the date one year ago from today
    one_year_ago = (datetime.now() - timedelta(days=365)).date()
    
    if 'full_data' not in st.session_state:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        client = bigquery.Client(credentials=credentials)
        # Modify the query
        query = f"""
        SELECT * FROM `sunpower-375201.sunpower_agg.sunpower_full_funnel` 
        WHERE Date BETWEEN '{one_year_ago}' AND CURRENT_DATE() """
        st.session_state.full_data = pandas.read_gbq(query, credentials=credentials)

    
    # Initialize the start and end date to the last 30 days
    if 'start_date' not in st.session_state:
        st.session_state['start_date'] = (datetime.now() - timedelta(days=30)).date()
    
    if 'end_date' not in st.session_state:
        st.session_state['end_date'] = datetime.now().date()
    
    #Select the start and end dates independently
    start_date = st.date_input(
        "Select start date",
        value=st.session_state['start_date'],
        min_value=one_year_ago,
        max_value=st.session_state['end_date']  # Ensure start date is not after end date
    )

    end_date = st.date_input(
        "Select end date",
        value=st.session_state['end_date'],
        min_value=start_date,  # Ensure end date is not before start date
        max_value=datetime.now().date()
    )


    #Set up Channel Filter
    if 'channels_unique' not in st.session_state:
        st.session_state.channels_unique = list(st.session_state.full_data["Channel_Non_Truth"].unique())
        # Initialize selected channels to all channels
        st.session_state.selected_channels = st.session_state.channels_unique
    
    with st.expander("Filter Channel"):
        selected_channels = [channel for channel in st.session_state.channels_unique 
                            if st.checkbox(channel, value=(channel in st.session_state.selected_channels), key=channel)]
    # Set up Type filter
    if 'types_unique' not in st.session_state:
        st.session_state.types_unique = list(st.session_state.full_data["Type"].unique())
        # Initialize selected types to all types
        st.session_state.selected_types = st.session_state.types_unique
    
    with st.expander("Filter Type"):
        selected_types = [typ for typ in st.session_state.types_unique if st.checkbox(typ, value=(typ in st.session_state.selected_types), key="type_" + typ)]

    # Fill NaN values in 'State_Name' with a placeholder like 'Not Entered'
    st.session_state.full_data['State_Name'].fillna('Not Entered', inplace=True)
    
    # Set up State Filter
    if 'states_unique' not in st.session_state:
        st.session_state.states_unique = list(st.session_state.full_data["State_Name"].unique())
        st.session_state.selected_states = st.session_state.states_unique.copy()
        st.session_state.interim_selected_states = st.session_state.selected_states.copy()  # Initialize it here
    
    with st.expander("Filter State"):
        # Ensure initialization for safety
        if 'interim_selected_states' not in st.session_state:
            st.session_state.interim_selected_states = st.session_state.selected_states.copy()
        
        # Toggle button
        if st.button("Select All States" if len(st.session_state.interim_selected_states) < len(st.session_state.states_unique) else "Clear All States"):
            if len(st.session_state.interim_selected_states) < len(st.session_state.states_unique):
                st.session_state.interim_selected_states = st.session_state.states_unique.copy()
            else:
                st.session_state.interim_selected_states = []
                
        selected_states = []
        for state in st.session_state.states_unique:
            if st.checkbox(state, value=(state in st.session_state.interim_selected_states)):
                selected_states.append(state)
        if selected_states:
            st.session_state.interim_selected_states = selected_states

    # Replace null values in 'Campaign' with 'Not Entered'
    st.session_state.full_data['Campaign'].fillna('Not Entered', inplace=True)
    
    # Set up Campaign Filter
    if 'campaigns_unique' not in st.session_state:
        st.session_state.campaigns_unique = list(st.session_state.full_data["Campaign"].unique())
        st.session_state.selected_campaigns = st.session_state.campaigns_unique.copy()
        st.session_state.interim_selected_campaigns = st.session_state.selected_campaigns.copy()  # Initialize it here
        
    with st.expander("Filter Campaign"):
        # Ensure initialization for safety
        if 'interim_selected_campaigns' not in st.session_state:
            st.session_state.interim_selected_campaigns = st.session_state.selected_campaigns.copy()
        
        # Toggle button
        if st.button("Select All Campaigns" if len(st.session_state.interim_selected_campaigns) < len(st.session_state.campaigns_unique) else "Clear All Campaigns"):
            if len(st.session_state.interim_selected_campaigns) < len(st.session_state.campaigns_unique):
                st.session_state.interim_selected_campaigns = st.session_state.campaigns_unique.copy()
            else:
                st.session_state.interim_selected_campaigns = []
                
        selected_campaigns = []
        for index, campaign in enumerate(st.session_state.campaigns_unique):
            # Use the index to generate a unique key for each checkbox
            if st.checkbox(campaign, value=(campaign in st.session_state.interim_selected_campaigns), key=f"campaign_{index}"):
                selected_campaigns.append(campaign)
        if selected_campaigns:
            st.session_state.interim_selected_campaigns = selected_campaigns
            
    if st.button("Re-run"):
        data = st.session_state.full_data.copy()
        st.session_state.selected_campaigns = st.session_state.interim_selected_campaigns.copy()
        st.session_state.selected_states = st.session_state.interim_selected_states.copy()
        st.session_state.selected_channels = selected_channels
        st.session_state.selected_types = selected_types
        st.session_state['start_date'] = start_date
        st.session_state['end_date'] = end_date
    

    # Start with the full dataset
    data = st.session_state.full_data.copy()
    
   # Define the filters
    data = data[(data['Date'] >= st.session_state['start_date']) & (data['Date'] <= st.session_state['end_date'])]
    channel_filter = data["Channel_Non_Truth"].isin(st.session_state.selected_channels)
    type_filter = data["Type"].isin(st.session_state.selected_types)
    state_filter = data["State_Name"].isin(st.session_state.selected_states)
    campaign_filter = data["Campaign"].isin(st.session_state.selected_campaigns)
    
    # Apply all filters at once
    data = data[channel_filter & type_filter & state_filter & campaign_filter]
    
    #### Metrics ####
    st.markdown("<h2 style='text-align: center;'>Metrics</h2>", unsafe_allow_html=True)
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
    
    ## Metric Selector for charts
    col7, col8 = st.columns(2)
    selection = 'Appts'
    options = ["Appointments", "Leads", "DQ", "Cost", "CPL", "CPA"]
    
    with col7:
        selection = st.selectbox("Select a metric:", options)

    with col8:
        st.write("*Double click on any reason in the legend to filter, single click to filter out")

        
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
    aggregated_data = data.groupby('State_Abbreviation').agg({col: 'sum' for col in data.columns if pd.api.types.is_numeric_dtype(data[col])}).reset_index()

    aggregated_data = aggregated_data.rename(columns={'Appts': 'Appointments', "Number_of_reports__Salesforce_Reports" : "Leads"})
    aggregated_data['CPL'] = aggregated_data['Cost']/aggregated_data['Leads']
    aggregated_data['CPA'] = aggregated_data['Cost']/aggregated_data['Appointments']
    
    with bottom_left_column:
        #Map showing leads by state
        fig_map = px.choropleth(aggregated_data, 
                        locations='State_Abbreviation', 
                        locationmode='USA-states', 
                        color= selection, 
                        scope='usa', 
                        title= f'{selection} by State',
                        color_continuous_scale='Viridis',
                        labels={selection:selection})
        st.plotly_chart(fig_map, use_container_width=True)
    
    with bottom_right_column:
        # Scatter plot showing Conversions as a function of clicks with a regression line
        fig_bar = px.bar(data, x='Date', y='DQ', color='Reason__Salesforce_Reports', title='DQ by Day')
        st.plotly_chart(fig_bar, use_container_width=True)   
    
if __name__ == '__main__':
    password_protection()
