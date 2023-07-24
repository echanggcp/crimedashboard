import pandas as pd 
import geopandas as gpd
from shapely.geometry import Point
import panel as pn
import hvplot.pandas
from panel.interact import interact
pn.extension('tabulator')
pn.extension()
import holoviews as hv
import io
import os
import requests


#################################################### SETUP ######################################################

#relevant variables
vars = ['ADDR_PCT_CD', 'OFNS_DESC', 'BORO_NM', 'Latitude', 'Longitude', 'geometry', 'Date', 'Year', 'Month']

# Relevant offense types
relevant_offense_types = [
    'BURGLARY',
    'ROBBERY',
    'FELONY ASSAULT',
    'GRAND LARCENY',
    'GRAND LARCENY OF MOTOR VEHICLE',
    'PETIT LARCENY',
    'MURDER & NON-NEGL. MANSLAUGHTER',
    'RAPE',
]

#function to make data more readable
def process_data(data):
    geometry = [Point(x, y) for x, y in zip(data['Longitude'], data['Latitude'])]
    data = gpd.GeoDataFrame(data, geometry=geometry)
    data['Date'] = pd.to_datetime(data['RPT_DT'])
    data['Year'] = data['Date'].dt.year
    data['Month'] = data['Date'].dt.month
    data = data[vars]
    data = data.rename(columns={'ADDR_PCT_CD': 'Precinct', 'OFNS_DESC': 'Offense', 'BORO_NM': 'Borough'})
    data = data[data['Offense'].isin(relevant_offense_types)]
    return data


##################################################### COMPLAINTS YTD ######################################################

url = 'https://data.cityofnewyork.us/api/views/5uac-w243/rows.csv?accessType=DOWNLOAD'
complaints_ytd = pd.read_csv(url)
complaints_ytd = process_data(complaints_ytd)
complaints_ytd = complaints_ytd[complaints_ytd['Year'] == 2023]
num_months = complaints_ytd['Month'].nunique()

##################################################### COMPLAINTS HISTORIC ######################################################

# Initialize historic data
complaints_historic = pd.read_csv("./RawData/Complaints_Historic.csv.csv")
complaints_historic = process_data(complaints_historic)

##################################################### BIDS ######################################################

#creates a dataframe from the shapefile for all BIDs
bids = gpd.read_file("./Shapefiles/bids/geo_export_b6ede37e-c4a1-45e6-ab3c-def45e8d22a3.shp")

#creates a dataframe from 'bids' for three BIDs
gcp = bids[bids.bid == "Grand Central Partnership"]
adny = bids[bids.bid == "Downtown Alliance BID"]
ts = bids[bids.bid == "Times Square BID"]
#gcp.plot()

#creates a column for the distance from each bid in YTD
complaints_ytd['dist_fromGCP'] = complaints_ytd.geometry.distance(gcp.geometry.iloc[0])
complaints_ytd['dist_fromADNY'] = complaints_ytd.geometry.distance(adny.geometry.iloc[0])
complaints_ytd['dist_fromTS'] = complaints_ytd.geometry.distance(ts.geometry.iloc[0])

#creates a column for the distance from each bid in historic
complaints_historic['dist_fromGCP'] = complaints_historic.geometry.distance(gcp.geometry.iloc[0])
complaints_historic['dist_fromADNY'] = complaints_historic.geometry.distance(adny.geometry.iloc[0])
complaints_historic['dist_fromTS'] = complaints_historic.geometry.distance(ts.geometry.iloc[0])

################################################ COMPLAINTS HISTORIC YTD ###########################################################

#creates historic ytd dataframe based on months in YTD dataframe
complaints_historic_ytd = complaints_historic[complaints_historic['Month'] <= num_months]

################################# CONCATINATE ###########################################################

#concatenates historic and ytd dataframes for ytd time periods
complaints_ytd_concat = pd.concat([complaints_ytd, complaints_historic_ytd])
#concatenates historic and ytd dataframes for full year time periods
complaints_historic_concat = pd.concat([complaints_historic, complaints_ytd])
# Create a new variable 'mon_yr' with only the month and year
complaints_historic_concat['mon_yr'] = complaints_historic_concat['Date'].dt.to_period('M')

################################################ BID specific dataframes ###########################################################

#creates a YTD dataframe for each BID
gcp_ytd = complaints_ytd_concat[complaints_ytd_concat['dist_fromGCP'] <= 0.0002]
adny_ytd = complaints_ytd_concat[complaints_ytd_concat['dist_fromADNY'] <= 0.0002]
ts_ytd = complaints_ytd_concat[complaints_ytd_concat['dist_fromTS'] <= 0.0002]

#creates a full year dataframe for each BID
gcp_fullyear = complaints_historic_concat[complaints_historic_concat['dist_fromGCP'] <= 0.0002]
adny_fullyear = complaints_historic_concat[complaints_historic_concat['dist_fromADNY'] <= 0.0002]
ts_fullyear = complaints_historic_concat[complaints_historic_concat['dist_fromTS'] <= 0.0002]

#creates YTD and full year dataframes for police precincts 14, 17, and 18
pcts_ytd = complaints_ytd_concat[complaints_ytd_concat['Precinct'].isin([14, 17, 18])]
pcts_fullyear = complaints_historic_concat[complaints_historic_concat['Precinct'].isin([14, 17, 18])]

pcts_fullyear['BID'] = pcts_fullyear['dist_fromGCP'] <= 0.0002


#################################################### SUMMARY STATS (ytd) ###########################################################

#NYC
summarytable_ytd = complaints_ytd_concat.pivot_table(index='Offense', columns='Year', aggfunc='size', fill_value=0)

#GCP
summarytable_ytd_gcp = gcp_ytd.pivot_table(index='Offense', columns='Year', aggfunc='size', fill_value=0)

#ADNY
summarytable_ytd_adny = adny_ytd.pivot_table(index='Offense', columns='Year', aggfunc='size', fill_value=0)

#TS
summarytable_ytd_ts = ts_ytd.pivot_table(index='Offense', columns='Year', aggfunc='size', fill_value=0)

#Pcts 14, 17, and 18
summarytable_ytd_pcts = pcts_ytd.pivot_table(index='Offense', columns='Year', aggfunc='size', fill_value=0)

#################################################### SUMMARY STATS (historic) ###########################################################

def pivot(df):
    return df.pivot_table(index='Offense', columns='Year', aggfunc='size', fill_value=0)


#NYC full year
summarytable_historic = pivot(complaints_historic_concat)

#GCP full year
summarytable_historic_gcp = pivot(gcp_fullyear)

#ADNY full year
summarytable_historic_adny = pivot(adny_fullyear)

#TS full year
summarytable_historic_ts = pivot(ts_fullyear)

#Pcts 14, 17, and 18 full year
summarytable_historic_pcts = pivot(pcts_fullyear)

summarytable_monthly_byoffense = complaints_historic_concat.pivot_table(index='Offense', columns=['Year', 'Month'], aggfunc='size', fill_value=0)
summarytable_monthly_gcp_byoffense = gcp_fullyear.pivot_table(index='Offense', columns=['Year', 'Month'], aggfunc='size', fill_value=0)
summarytable_monthly_pct_byoffense = pcts_fullyear.pivot_table(index='Offense', columns=['Year', 'Month'], aggfunc='size', fill_value=0)

crime_counts_pcts = pcts_fullyear.groupby(pcts_fullyear['mon_yr']).size().reset_index(name='total_crimes_inpcts')
crime_counts_gcp = gcp_fullyear.groupby(gcp_fullyear['mon_yr']).size().reset_index(name='total_crimes_ingcp')
crime_counts_adny = adny_fullyear.groupby(adny_fullyear['mon_yr']).size().reset_index(name='total_crimes_inadny')
crime_counts_ts = ts_fullyear.groupby(ts_fullyear['mon_yr']).size().reset_index(name='total_crimes_ints')

crime_counts_pctvsgcp = pd.merge(crime_counts_pcts, crime_counts_gcp, on='mon_yr', how='outer')
crime_counts_bids = pd.merge(crime_counts_gcp, crime_counts_adny, on='mon_yr', how='outer')
crime_counts_bids = pd.merge(crime_counts_bids, crime_counts_ts, on='mon_yr', how='outer')

#################################################### WRITING TO EXCEL WORKBOOK ###########################################################

#writer = pd.ExcelWriter(r"C:\Users\echang\OneDrive - Grand Central Partnership\Desktop\Python Data Analytics\crime_stats_automated.xlsx", mode='a', engine='openpyxl')
#workbook = writer.book

#summarytable_historic.to_excel(writer, sheet_name = 'Full Year NYC')
#summarytable_ytd.to_excel(writer, sheet_name = 'YTD NYC')

#writer._save()

###################################################### Covert columns to strings ###########################################################


def convert_columns_to_string(data):
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.map(lambda x: tuple(str(i) for i in x))
    else:
        data.columns = data.columns.astype(str)
    return data

# Apply the function to the DataFrames
complaints_ytd_concat = convert_columns_to_string(complaints_ytd_concat)
complaints_historic_concat = convert_columns_to_string(complaints_historic_concat)
gcp_ytd = convert_columns_to_string(gcp_ytd)
adny_ytd = convert_columns_to_string(adny_ytd)
ts_ytd = convert_columns_to_string(ts_ytd)
gcp_fullyear = convert_columns_to_string(gcp_fullyear)
adny_fullyear = convert_columns_to_string(adny_fullyear)
ts_fullyear = convert_columns_to_string(ts_fullyear)
pcts_ytd = convert_columns_to_string(pcts_ytd)
pcts_fullyear = convert_columns_to_string(pcts_fullyear)
summarytable_ytd = convert_columns_to_string(summarytable_ytd)
summarytable_ytd_gcp = convert_columns_to_string(summarytable_ytd_gcp)
summarytable_ytd_adny = convert_columns_to_string(summarytable_ytd_adny)
summarytable_ytd_ts = convert_columns_to_string(summarytable_ytd_ts)
summarytable_ytd_pcts = convert_columns_to_string(summarytable_ytd_pcts)
summarytable_monthly_gcp_byoffense = convert_columns_to_string(summarytable_monthly_gcp_byoffense)
summarytable_monthly_pct_byoffense = convert_columns_to_string(summarytable_monthly_pct_byoffense)
summarytable_historic = convert_columns_to_string(summarytable_historic)
summarytable_historic_pcts = convert_columns_to_string(summarytable_historic_pcts)
summarytable_historic_gcp = convert_columns_to_string(summarytable_historic_gcp)
summarytable_historic_adny = convert_columns_to_string(summarytable_historic_adny)
summarytable_historic_ts = convert_columns_to_string(summarytable_historic_ts)


##################################################### Widgets ###########################################################

YTDvsFullyear = pn.widgets.RadioButtonGroup(name = 'Time Period', options = ['YTD', 'Full Year'], button_type = 'danger', inline=True, sizing_mode = 'stretch_width')
region = pn.widgets.RadioButtonGroup(name = 'Area of Interest', options = ['NYC', 'PCTs 14, 17, 18', 'GCP', 'ADNY', 'TS'], button_type = 'danger', inline=True, sizing_mode = 'stretch_width')

first_year = pn.widgets.Select(name = 'First Year', options = ['2018','2019','2020','2021','2022','2023'], sizing_mode = 'stretch_width')
second_year = pn.widgets.Select(name = 'Second Year', options = ['2018','2019','2020','2021','2022','2023'], sizing_mode = 'stretch_width')
year = pn.widgets.Select(name = 'Year', options = ['2018','2019', '2020', '2021', '2022', '2023'], sizing_mode = 'stretch_width')


##################################################### Tables ###########################################################

dict = {'YTD':0, 'Full Year':1}
dictionary = {'NYC':[summarytable_ytd, summarytable_historic], 'PCTs 14, 17, 18':[summarytable_ytd_pcts, summarytable_historic_pcts], 'GCP':[summarytable_ytd_gcp, summarytable_historic_gcp], 'ADNY':[summarytable_ytd_adny, summarytable_historic_adny], 'TS':[summarytable_ytd_ts, summarytable_historic_ts]}

def update_table(first_year, second_year, df_key, df_key2):
    i = dict[df_key]
    df = dictionary[df_key2][i]
    table = df[[first_year, second_year]]
    table.loc['Total'] = table.sum()
    table['% Change'] = (((table.iloc[:, 1] - table.iloc[:, 0]) / table.iloc[:, 0]) * 100).round(1)
    return table

def update_table2(year, df_key, df_key2):
    i = dict[df_key]
    df = dictionary[df_key2][i]
    table = df[[year]]
    total_crimes = table[year].sum()
    table[str(year)] = ((table[year] / total_crimes) * 100).round(2)
    table = table[[str(year)]]
    table.loc['Total'] = 100
    return table

table_pane = pn.panel(pn.bind(update_table, first_year, second_year, YTDvsFullyear, region), sizing_mode = 'stretch_width', center = True)    
table2_pane = pn.panel(pn.bind(update_table2, year, YTDvsFullyear, region), sizing_mode = 'stretch_width', center = True)


########################################################### Download widgets ###########################################################

def generate_table1_dataframe():
    first_year_value = first_year.value
    second_year_value = second_year.value
    YTDvsFullyear_value = YTDvsFullyear.value
    region_value = region.value
    df = pd.DataFrame(update_table(first_year_value, second_year_value, YTDvsFullyear_value, region_value))
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=True)
    csv_buffer.seek(0)
    return csv_buffer

def generate_table2_dataframe():
    year_value = year.value
    YTDvsFullyear_value = YTDvsFullyear.value
    region_value = region.value
    df = pd.DataFrame(update_table2(year_value, YTDvsFullyear_value, region_value))
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=True)
    csv_buffer.seek(0)
    return csv_buffer

download_table1_button = pn.widgets.FileDownload(
    callback=generate_table1_dataframe, filename="totals.csv", button_type="danger", label="Download"
)

download_table2_button = pn.widgets.FileDownload(
    callback=generate_table2_dataframe, filename="proportion.csv", button_type="danger", label="Download"
)


##################################################### Plots ###########################################################


pctvsgcp_plot = crime_counts_pctvsgcp.hvplot.line(x='mon_yr', y='total_crimes_inpcts', label='Precincts 14, 17, 18', title='Crime Counts by Month', line_width=2, shared_axes=False, xlabel = 'Time (Month)', ylabel= 'Total Crimes') * crime_counts_pctvsgcp.hvplot.line(x='mon_yr', y='total_crimes_ingcp', label='GCP', xlabel = 'Time (Month)', ylabel= 'Total Crimes', line_width=2, shared_axes=False)

bids_plot = crime_counts_bids.hvplot.line(x='mon_yr', y='total_crimes_ingcp', label='GCP', xlabel = 'Time (Month)', ylabel= 'Total Crimes', title='Crime Counts by Month', line_width=4, shared_axes=False) * crime_counts_bids.hvplot.line(x='mon_yr', y='total_crimes_inadny', label='ADNY', xlabel = 'Time (Month)', ylabel= 'Total Crimes', line_width=2, shared_axes=False) * crime_counts_bids.hvplot.line(x='mon_yr', y='total_crimes_ints', label='TS', line_width=2, shared_axes=False, xlabel = 'Time (Month)', ylabel= 'Total Crimes')

''' Axis scaling but legend labels are not ideal
pctvsgcp_plot = crime_counts_pctvsgcp.hvplot.line(
    x='mon_yr', y=['total_crimes_inpcts', 'total_crimes_ingcp'], ylim=(0, 1500), title='Crime Counts by Month',
    legend_position='bottom', line_width=2, shared_axes=False
)
bids_plot = crime_counts_bids.hvplot.line(
    x='mon_yr', y=['total_crimes_ingcp', 'total_crimes_inadny', 'total_crimes_ints'], ylim=(0, 400), title='Crime Counts by Month',
    legend_position='bottom', line_width=2, shared_axes=False
)
'''

###################################################### Images ############################################################

banner = pn.pane.JPG(
    "./Images/GCP_banner.jpg",
    width = 300
)

map = pn.pane.JPG(
    "./Images/JPG of GCP Official Neighborhood Map_2018_3.jpg",
    width = 300
)

###################################################### Dashboard ############################################################

# Create a Panel dashboard
dashboard = pn.template.FastListTemplate(
    title = "Crime Statistics Dashboard",
    sidebar = [banner, pn.Row(height=50),
               pn.pane.Markdown("### In order to effectively evaluate the safety of Grand Central Partnership, it is important to compare crime in the GCP to other areas. Toggle through the settings to discover crime trends."),
               pn.pane.Markdown("#### Note: YTD statistics are based on the first " + str(num_months) + " months of each calendar year. Data is taken from NYC Open Data's NYPD Complaints datasets."),
               pn.Row(height=100), map],
    main = [pn.Row(pn.panel(pctvsgcp_plot, sizing_mode='stretch_width'), pn.panel(bids_plot, sizing_mode='stretch_width')),
            pn.Row(pn.Column(region, YTDvsFullyear, pn.Row(pn.Column(pn.Row(first_year, second_year), pn.pane.Markdown('### Crime Counts'), table_pane, download_table1_button), pn.Column(width = 80), pn.Column(pn.Row(year), pn.pane.Markdown('### Crimes by Type as a Percentage of Total Crimes'), table2_pane, download_table2_button)), sizing_mode='stretch_width'))],
    accent_base_color = '#C82333',
    header_background = '#C82333'
)

# Show the dashboard
dashboard.servable()
