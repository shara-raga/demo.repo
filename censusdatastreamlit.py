import streamlit as st
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

# MySQL connection
db_engine = create_engine('mysql+mysqlconnector://root:@localhost/project1')

# Define queries
queries = {
    "Total population of each district": """
        SELECT `District code`, `District`, SUM(Population) AS Total_Population 
        FROM census 
        GROUP BY `District code`, `District`
    """,
    "Literate males and females in each district": """
        SELECT `District code`, `District`, SUM(Literate_Male) AS Literate_Males, SUM(Literate_Female) AS Literate_Females 
        FROM census 
        GROUP BY `District code`, `District`
    """,
    "Percentage of workers (both male and female) in each district": """
        SELECT `District code`, `District`, (SUM(Workers) / SUM(Population)) * 100 AS Percentage_Workers 
        FROM census 
        GROUP BY `District code`, `District`
    """,
    "Households with access to LPG or PNG as a cooking fuel in each district": """
        SELECT `District code`, `District`, SUM(LPG_or_PNG_Households) AS LPG_PNG_Households 
        FROM census 
        GROUP BY `District code`, `District`
    """,
    "Religious composition of each district": """
        SELECT `District code`, `District`, SUM(Hindus) AS Hindus, SUM(Muslims) AS Muslims, SUM(Christians) AS Christians, SUM(Sikhs) AS Sikhs, SUM(Buddhists) AS Buddhists, SUM(Jains) AS Jains, SUM(Others_Religions) AS Others, SUM(Religion_Not_Stated) AS Religion_Not_Stated 
        FROM census 
        GROUP BY `District code`, `District`
    """,
    "Households with internet access in each district": """
        SELECT `District code`, `District`, SUM(Households_with_Internet) AS Households_with_Internet 
        FROM census 
        GROUP BY `District code`, `District`
    """,
    "Educational attainment distribution in each district": """
        SELECT `District code`, `District`, SUM(Below_Primary_Education) AS Below_Primary, SUM(Primary_Education) AS Primary, SUM(Middle_Education) AS Middle, SUM(Secondary_Education) AS Secondary, SUM(Higher_Education) AS Higher, SUM(Graduate_Education) AS Graduate 
        FROM census 
        GROUP BY `District code`, `District`
    """,
    "Households with access to various modes of transportation in each district": """
        SELECT `District code`, `District`, SUM(Households_with_Bicycle) AS Bicycle, SUM(Households_with_Car_Jeep_Van) AS Car_Jeep_Van, SUM(Households_with_Radio_Transistor) AS Radio_Transistor, SUM(Households_with_Television) AS Television 
        FROM census 
        GROUP BY `District code`, `District`
    """,
    "Condition of occupied census houses in each district": """
        SELECT `District code`, `District`, SUM(Condition_of_occupied_census_houses_Dilapidated_Households) AS Dilapidated, SUM(Households_with_separate_kitchen_Cooking_inside_house) AS Separate_Kitchen, SUM(Having_bathing_facility_Total_Households) AS Bathing_Facility, SUM(Having_latrine_facility_within_the_premises_Total_Households) AS Latrine_Facility 
        FROM census 
        GROUP BY `District code`, `District`
    """,
    "Household size distribution in each district": """
        SELECT `District code`, `District`, SUM(Household_size_1_person_Households) AS Size_1, SUM(Household_size_2_persons_Households) AS Size_2, SUM(Household_size_3_persons_Households) AS Size_3, SUM(Household_size_3_to_5_persons_Households) AS Size_3_to_5, SUM(Household_size_4_persons_Households) AS Size_4, SUM(Household_size_5_persons_Households) AS Size_5, SUM(Household_size_6_8_persons_Households) AS Size_6_to_8, SUM(Household_size_9_persons_and_above_Households) AS Size_9_and_above 
        FROM census 
        GROUP BY `District code`, `District`
    """,
    "Total number of households in each state": """
        SELECT `State/UT`, SUM(Households) AS Total_Households 
        FROM census 
        GROUP BY `State/UT`
    """,
    "Households with a latrine facility within the premises in each state": """
        SELECT `State/UT`, SUM(Having_latrine_facility_within_the_premises_Total_Households) AS Latrine_Facility 
        FROM census 
        GROUP BY `State/UT`
    """,
    "Average household size in each state": """
        SELECT `State/UT`, AVG(Household_size_3_to_5_persons_Households + Household_size_1_person_Households + Household_size_2_persons_Households + Household_size_4_persons_Households + Household_size_5_persons_Households + Household_size_6_8_persons_Households + Household_size_9_persons_and_above_Households) AS Avg_Household_Size 
        FROM census 
        GROUP BY `State/UT`
    """,
    "Households owned versus rented in each state": """
        SELECT `State/UT`, SUM(Ownership_Owned_Households) AS Owned, SUM(Ownership_Rented_Households) AS Rented 
        FROM census 
        GROUP BY `State/UT`
    """,
    "Distribution of different types of latrine facilities in each state": """
        SELECT `State/UT`, SUM(Type_of_latrine_facility_Pit_latrine_Households) AS Pit_Latrine, SUM(Type_of_latrine_facility_Other_latrine_Households) AS Other_Latrine, SUM(Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Households) AS Night_Soil, SUM(Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households) AS Flush_Latrine 
        FROM census 
        GROUP BY `State/UT`
    """,
    "Households with access to drinking water sources near the premises in each state": """
        SELECT `State/UT`, SUM(Location_of_drinking_water_source_Near_the_premises_Households) AS Near_Premises 
        FROM census 
        GROUP BY `State/UT`
    """,
    "Average household income distribution in each state based on power parity categories": """
        SELECT `State/UT`, AVG(Power_Parity_Less_than_Rs_45000 + Power_Parity_Rs_45000_90000 + Power_Parity_Rs_90000_150000 + Power_Parity_Rs_45000_150000 + Power_Parity_Rs_150000_240000 + Power_Parity_Rs_240000_330000 + Power_Parity_Rs_150000_330000 + Power_Parity_Rs_330000_425000 + Power_Parity_Rs_425000_545000 + Power_Parity_Rs_330000_545000 + Power_Parity_Above_Rs_545000) AS Avg_Household_Income 
        FROM census 
        GROUP BY `State/UT`
    """,
    "Percentage of married couples with different household sizes in each state": """
        SELECT `State/UT`, (SUM(Married_couples_1_Households) / SUM(Households)) * 100 AS Married_Couples_1, (SUM(Married_couples_2_Households) / SUM(Households)) * 100 AS Married_Couples_2, (SUM(Married_couples_3_Households) / SUM(Households)) * 100 AS Married_Couples_3, (SUM(Married_couples_3_or_more_Households) / SUM(Households)) * 100 AS Married_Couples_3_or_more 
        FROM census 
        GROUP BY `State/UT`
    """,
    "Households falling below the poverty line in each state based on the power parity categories": """
        SELECT `State/UT`, SUM(Power_Parity_Less_than_Rs_45000) AS Below_Poverty_Line 
        FROM census 
        GROUP BY `State/UT`
    """,
    "Overall literacy rate (percentage of literate population) in each state": """
        SELECT `State/UT`, (SUM(Literate) / SUM(Population)) * 100 AS Literacy_Rate 
        FROM census 
        GROUP BY `State/UT`
    """,
}

# Streamlit app
st.title("Census Data Analysis")

# Dropdown for selecting the query
query_choice = st.selectbox("Select Query to Run", list(queries.keys()))

# Run selected query
if query_choice:
    query = queries[query_choice]
    result = pd.read_sql(query, db_engine)
    st.write(result)
