#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install pyspark')


# In[21]:


from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,FloatType
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("App").getOrCreate()


# In[15]:


schema=StructType([
    StructField("product_id", IntegerType() , True),
    StructField("customer_id", StringType() , True),
    StructField("order_date", DateType() , True),
    StructField("location", StringType() , True),
    StructField("source_order", StringType() , True)

])


# In[ ]:





# In[16]:


sales_df=spark.read.format("csv").option("inferschema","true").schema(schema).load("sales.csv.txt")
sales_df.show()


# In[17]:


#deriving year , month , quarter
from pyspark.sql.functions import month , year , quarter
sales_df=sales_df.withColumn("order_year",year(sales_df.order_date))
sales_df.show()


# In[19]:


sales_df=sales_df.withColumn("order_month",month(sales_df.order_date))
sales_df=sales_df.withColumn("order_quarter",quarter(sales_df.order_date))
sales_df.show(sales_df.count())


# In[22]:


#menu dataframe
menuSchema=StructType([
    StructField("product_id", IntegerType() , True),
    StructField("product_name", StringType() , True),
    StructField("price", FloatType() , True),
])
menu_df=spark.read.format("csv").option("inferschema","true").schema(menuSchema).load("menu.csv.txt")
menu_df.show()


# In[28]:


#total ammount spent by each customer
#we have to join the 2 dataframes
#then we have to group based on customer id
total_ammount_spent=(sales_df.join(menu_df,'product_id').groupBy('customer_id').agg({'price':'sum'}).orderBy('customer_id'))
total_ammount_spent.show()


# In[31]:


#data visualization
get_ipython().system('pip install matplotlib')
import matplotlib.pyplot as plt
# Collectez les données en tant que liste de lignes
data = total_ammount_spent.collect()
print(data)


# In[32]:


# Extraitz les valeurs de montant total dépensé et les IDs des clients
customer_ids = [row.customer_id for row in data]
total_amounts = [row['sum(price)'] for row in data]
# Créez un graphique à barres pour visualiser les données
plt.figure(figsize=(10, 6))
plt.bar(customer_ids, total_amounts)
plt.xlabel('Customer ID')
plt.ylabel('Total Amount Spent')
plt.title('Total Amount Spent by Customer')
plt.show()


# In[35]:


#total ammount sold by each product
#we have to join the 2 dataframes
#then we have to group based on product id
total_ammount_spent_by_product=(sales_df.join(menu_df,'product_id').groupBy('product_name').agg({'price':'sum'}).orderBy('product_name'))
total_ammount_spent_by_product.show()


# In[36]:


#visualisation
# Collectez les données en tant que liste de lignes
data = total_ammount_spent_by_product.collect()# Extraitz les valeurs de montant total dépensé et les IDs des clients
product_names = [row.product_name for row in data]
total_amounts = [row['sum(price)'] for row in data]
# Créez un graphique à barres pour visualiser les données
plt.figure(figsize=(10, 6))
plt.bar(product_names, total_amounts)
plt.xlabel('Product name')
plt.ylabel('Total Amount Spent')
plt.title('Total Amount Spent by Product')
plt.show()


# In[37]:


#total ammount of sales in each month
#we have to join the 2 dataframes
#then we have to group based on order month 
total_ammount_spent_by_month=(sales_df.join(menu_df,'product_id').groupBy('order_month').agg({'price':'sum'}).orderBy('order_month'))
total_ammount_spent_by_month.show()


# In[38]:


#visualisation
# Collectez les données en tant que liste de lignes
data = total_ammount_spent_by_month.collect()# Extraitz les valeurs de montant total dépensé et les IDs des clients
order_month = [row.order_month for row in data]
total_amounts = [row['sum(price)'] for row in data]
# Créez un graphique à barres pour visualiser les données
plt.figure(figsize=(10, 6))
plt.bar(order_month, total_amounts)
plt.xlabel('Order month')
plt.ylabel('Total Amount Spent')
plt.title('Total Amount Spent by mMnth')
plt.show()


# In[47]:


#yearly sales
#total ammount of sales in each month
#we have to join the 2 dataframes
#then we have to group based on order year 
total_ammount_spent_by_year=(sales_df.join(menu_df,'product_id').groupBy('order_year').agg({'price':'sum'}).orderBy('order_year'))
total_ammount_spent_by_year.show()


# In[49]:


#visualisation
# Collectez les données en tant que liste de lignes
data = total_ammount_spent_by_year.collect()# Extraitz les valeurs de montant total dépensé et les IDs des clients
order_year = [int(row.order_year) for row in data]
total_amounts = [row['sum(price)'] for row in data]
# Créez un graphique à barres pour visualiser les données
plt.figure(figsize=(10, 6))
plt.bar(order_year, total_amounts)
plt.xlabel('Order year')
plt.ylabel('Total Amount Spent')
plt.title('Total Amount Spent by Year')
plt.xticks(order_year, order_year)
plt.show()


# In[45]:


#quarter sales
#total ammount of sales in each month
#we have to join the 2 dataframes
#then we have to group based on order year 
total_ammount_spent_by_quarter=(sales_df.join(menu_df,'product_id').groupBy('order_quarter').agg({'price':'sum'}).orderBy('order_quarter'))
total_ammount_spent_by_quarter.show()


# In[51]:


#visualisation
# Collectez les données en tant que liste de lignes
data = total_ammount_spent_by_quarter.collect()# Extraitz les valeurs de montant total dépensé et les IDs des clients
order_quarter = [int(row.order_quarter) for row in data]
total_amounts = [row['sum(price)'] for row in data]
# Créez un graphique à barres pour visualiser les données
plt.figure(figsize=(10, 6))
plt.bar(order_quarter, total_amounts)
plt.xlabel('Order quarter')
plt.ylabel('Total Amount Spent')
plt.title('Total Amount Spent by Quarter')
plt.xticks(order_quarter, order_quarter)
plt.show()


# In[56]:


#how many times a product is purshased
most_df = (sales_df
    .join(menu_df, 'product_id')
    .groupBy('product_id', 'product_name')
    .agg(count('product_id').alias('product_count'))
)

# Sort the DataFrame by 'product_count' in descending order
most_df = most_df.orderBy('product_count', ascending=False).drop('product_id')
most_df.show()


# In[57]:


# Collect the data as a list of rows
data = most_df.collect()

# Extract the product names and their corresponding counts
product_names = [row.product_name for row in data]
product_counts = [row.product_count for row in data]

# Create a bar chart to visualize the data
plt.figure(figsize=(10, 6))
plt.barh(product_names, product_counts)  # Use barh for horizontal bars
plt.xlabel('Product Count')
plt.ylabel('Product Name')
plt.title('Most Purchased Products')

# Show the chart
plt.show()


# In[59]:


#top 5 ordered item
top_five_df = (sales_df
    .join(menu_df, 'product_id')
    .groupBy('product_id', 'product_name')
    .agg(count('product_id').alias('product_count'))
)

# Sort the DataFrame by 'product_count' in descending order
top_five_df = most_df.orderBy('product_count', ascending=False).drop('product_id').limit(5)
top_five_df.show()


# In[65]:


#frequency of visists to restaurant
from pyspark.sql.functions import countDistinct
df_visits=(sales_df.filter(sales_df.source_order=='Restaurant').groupBy('customer_id') .agg(countDistinct('order_date').alias('visit_count')))
df_visits.show()


# In[66]:


# Collect the data as a list of rows
data = df_visits.collect()

# Extract the customer IDs and their corresponding visit counts
customer_ids = [row.customer_id for row in data]
visit_counts = [row.visit_count for row in data]

# Create a bar chart to visualize the data
plt.figure(figsize=(10, 6))
plt.bar(customer_ids, visit_counts)
plt.xlabel('Customer ID')
plt.ylabel('Visit Count')
plt.title('Frequency of Visits to the Restaurant by Customer')

# Show the chart
plt.show()


# In[67]:


#total sales by each country
df_country=(sales_df.join(menu_df,'product_id').groupBy('location').agg({'price':'sum'}))
df_country.show()


# In[68]:


# Collect the data as a list of rows
data = df_country.collect()

# Extract the location (country) names and their corresponding total sales
locations = [row.location for row in data]
total_sales = [row['sum(price)'] for row in data]

# Create a bar chart to visualize the data
plt.figure(figsize=(10, 6))
plt.bar(locations, total_sales)
plt.xlabel('Location (Country)')
plt.ylabel('Total Sales')
plt.title('Total Sales by Location (Country)')

# Show the chart
plt.xticks(rotation=45)  # Rotate X-axis labels for better visibility
plt.show()


# In[69]:


#total sales by order source
df_source_order=(sales_df.join(menu_df,'product_id').groupBy('source_order').agg({'price':'sum'}))
df_source_order.show()


# In[70]:


# Collect the data as a list of rows
data = df_source_order.collect()

# Extract the source order types and their corresponding total sales
source_orders = [row.source_order for row in data]
total_sales = [row['sum(price)'] for row in data]

# Create a bar chart to visualize the data
plt.figure(figsize=(10, 6))
plt.bar(source_orders, total_sales)
plt.xlabel('Source Order')
plt.ylabel('Total Sales')
plt.title('Total Sales by Source Order')

# Show the chart
plt.xticks(rotation=45)  # Rotate X-axis labels for better visibility
plt.show()


# In[ ]:


get_ipython().system('git init')


# In[ ]:


get_ipython().system('git add .')


# In[ ]:


get_ipython().system('git commit -m "first commit"')


# In[ ]:


get_ipython().system('git remote add origin git@github.com:yosr-s/pyspark-sales.git')


# In[ ]:


get_ipython().system('git branch -M main')


# In[ ]:


get_ipython().system('git push -u origin main')


# In[ ]:




