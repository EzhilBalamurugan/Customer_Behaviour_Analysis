import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from textblob import TextBlob

# Database connection setup
engine = create_engine("mysql+mysqlconnector://root:Greninja123%40@localhost/customer_analysis")

# Load CSV data
customer_journey = pd.read_csv("customer_journey.csv")
customer_reviews = pd.read_csv("customer_reviews.csv")
customers = pd.read_csv("customers.csv")
engagement_data = pd.read_csv("engagement_data.csv")
geography = pd.read_csv("geography.csv")
products = pd.read_csv("products.csv")

# Ensure consistent casing for comparison
customer_journey["Stage"] = customer_journey["Stage"].str.lower()
customer_journey["Action"] = customer_journey["Action"].str.lower()

# Filter purchase data (acts as an orders table)
purchase_data = customer_journey[(customer_journey["Stage"] == "checkout") & (customer_journey["Action"] == "purchase")]

# Merge with customers and products to get meaningful insights
purchase_data = purchase_data.merge(customers, on="CustomerID", how="left")
purchase_data = purchase_data.merge(products, on="ProductID", how="left")

# Select relevant columns for analysis
purchase_data = purchase_data[["CustomerID", "CustomerName", "ProductID", "ProductName", "VisitDate"]]

# Store in MySQL
purchase_data.to_sql("purchases", engine, if_exists="replace", index=False)

# Identify drop-off points in the customer journey
drop_off_data = customer_journey[(customer_journey["Stage"] == "checkout") & (customer_journey["Action"] == "drop-off")]
drop_off_data.to_sql("drop_offs", engine, if_exists="replace", index=False)

# Identify highest-rated and lowest-rated products
customer_reviews["Sentiment"] = customer_reviews["ReviewText"].apply(lambda x: TextBlob(str(x)).sentiment.polarity)
highest_rated = customer_reviews.groupby("ProductID")["Rating"].mean().reset_index().sort_values("Rating", ascending=False)
lowest_rated = customer_reviews.groupby("ProductID")["Rating"].mean().reset_index().sort_values("Rating", ascending=True)

# Store review analysis in SQL
highest_rated.to_sql("highest_rated_products", engine, if_exists="replace", index=False)
lowest_rated.to_sql("lowest_rated_products", engine, if_exists="replace", index=False)

# Customer retention rate calculation
query_repeat_customers = """
    SELECT COUNT(DISTINCT CustomerID) AS RepeatCustomers
    FROM purchases
    WHERE CustomerID IN (
        SELECT CustomerID FROM purchases
        GROUP BY CustomerID
        HAVING COUNT(CustomerID) > 1
    )
"""
retention_rate = pd.read_sql(query_repeat_customers, engine)
print("Customer Retention Rate:")
print(retention_rate.to_string(index=False))

# First-time buyers calculation
query_first_time_buyers = """
    SELECT COUNT(DISTINCT CustomerID) AS FirstTimeBuyers
    FROM purchases
    WHERE CustomerID NOT IN (
        SELECT CustomerID FROM purchases
        GROUP BY CustomerID
        HAVING COUNT(CustomerID) > 1
    )
"""
first_time_buyers = pd.read_sql(query_first_time_buyers, engine)
print("First-Time Buyers:")
print(first_time_buyers.to_string(index=False))

# Generate business recommendations
if retention_rate.iloc[0]["RepeatCustomers"] > first_time_buyers.iloc[0]["FirstTimeBuyers"]:
    print("Recommendation: Focus on customer loyalty programs to further improve retention.")
else:
    print("Recommendation: Implement targeted marketing campaigns to convert first-time buyers into repeat customers.")

print("Customer journey and engagement analysis completed!")
