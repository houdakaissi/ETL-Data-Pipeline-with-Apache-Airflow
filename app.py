import pandas as pd
import psycopg2
import plotly.express as px
import streamlit as st

# Create a connection to your PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="air",  # Replace with your database name
    user="postgres",  # Replace with your database user
    password="123"    # Replace with your database password
)

# Query to get offices data
offices_query = "SELECT * FROM offices"
offices_df = pd.read_sql(offices_query, conn)

# Query to get employees data
employees_query = """
    SELECT e.firstName, e.lastName, e.jobTitle, o.city, o.state 
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
"""
employees_df = pd.read_sql(employees_query, conn)

# Query to get customers data
customers_query = "SELECT * FROM customers"
customers_df = pd.read_sql(customers_query, conn)

# Query to get orders data
orders_query = "SELECT * FROM orders"
orders_df = pd.read_sql(orders_query, conn)

# Query to get orderdetails data
orderdetails_query = "SELECT * FROM orderdetails"
orderdetails_df = pd.read_sql(orderdetails_query, conn)

# Close the database connection
conn.close()

# Streamlit UI components
st.title('Company Data Visualizations')

# Display tables in Streamlit
st.subheader("Offices Data")
st.write(offices_df.head())

# Visualize number of offices by city
fig1 = px.bar(offices_df, x='city', title="Number of Offices by City", color='city', 
              labels={'city': 'City', 'count': 'Number of Offices'})
st.plotly_chart(fig1)

# Display employees data
st.subheader("Employees by Office")
st.write(employees_df.head())

# Visualize number of employees by office city
fig2 = px.histogram(employees_df, x='city', title="Number of Employees by City", color='city',
                    labels={'city': 'City', 'count': 'Number of Employees'})
st.plotly_chart(fig2)

# Display customers data
st.subheader("Customers Data")
st.write(customers_df.head())

# Visualize number of customers by country
fig3 = px.bar(customers_df, x='country', title="Number of Customers by Country", color='country', 
              labels={'country': 'Country', 'count': 'Number of Customers'})
st.plotly_chart(fig3)

fig11 = px.scatter_geo(customers_df, locations="country", hover_name="city", title="Geographical Distribution of Customers",
                       color="country", color_continuous_scale="Viridis", projection="natural earth")
st.plotly_chart(fig11)

fig12 = px.bar(customers_df, x='state', title="Number of Customers by State", color='state',
               labels={'state': 'State', 'count': 'Number of Customers'})
st.plotly_chart(fig12)


# Visualize customers by postal code (if applicable)
# Visualize number of customers by postal code
fig20 = px.histogram(customers_df, x='postalcode', title="Number of Customers by Postal Code", color='postalcode',
                     labels={'postalcode': 'Postal Code', 'count': 'Number of Customers'})
st.plotly_chart(fig20)
fig_offices_by_country = px.bar(offices_df, x='country', title="Number of Offices by Country", color='country',
                                labels={'country': 'Country', 'count': 'Number of Offices'})
st.plotly_chart(fig_offices_by_country)
# Visualize number of employees by job title
# Visualize number of products by product line
productlines_query = "SELECT * FROM productlines"
productlines_df = pd.read_sql(productlines_query, conn)

# Visualize number of products by product line
fig1 = px.bar(productlines_df, x='productLine', title="Number of Products by Product Line", color='productLine')
st.plotly_chart(fig1)


