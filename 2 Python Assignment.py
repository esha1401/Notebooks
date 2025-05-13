# Databricks notebook source
# MAGIC %md
# MAGIC # 📝 Assignment: Python Collections
# MAGIC This notebook contains practice exercises on Python collections like List, Dictionary, Tuple, and Set.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Part 1: List Operations
# MAGIC Given the list of sales amounts:

# COMMAND ----------

sales = [250, 300, 400, 150, 500, 200]

# COMMAND ----------

# MAGIC %md
# MAGIC **Tasks:**
# MAGIC 1. Find the total sales.
# MAGIC 2. Calculate the average sale amount.
# MAGIC 3. Print sale values above 300.
# MAGIC 4. Add `350` to the list.
# MAGIC 5. Sort the list in descending order.

# COMMAND ----------

# Initial sales list
sales = [250, 300, 400, 150, 500, 200]

# 1. Find the total sales
total_sales = sum(sales)
print("Total Sales:", total_sales)

# 2. Calculate the average sale amount
average_sale = total_sales / len(sales)
print("Average Sale Amount:", average_sale)

# 3. Print sale values above 300
above_300 = [sale for sale in sales if sale > 300]
print("Sales Above 300:", above_300)

# 4. Add 350 to the list
sales.append(350)
print("Sales List After Adding 350:", sales)

# 5. Sort the list in descending order
sales.sort(reverse=True)
print("Sales List in Descending Order:", sales)


# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Part 2: Dictionary Operations
# MAGIC Create a dictionary with product names and their prices:

# COMMAND ----------

products = {
    "Laptop": 70000,
    "Mouse": 500,
    "Keyboard": 1500,
    "Monitor": 12000
}

# COMMAND ----------

# MAGIC %md
# MAGIC **Tasks:**
# MAGIC 1. Print the price of the "Monitor".
# MAGIC 2. Add a new product `"Webcam"` with price `3000`.
# MAGIC 3. Update the price of "Mouse" to `550`.
# MAGIC 4. Print all product names and prices using a loop.

# COMMAND ----------

# Initial dictionary of products and prices
products = {
    "Laptop": 70000,
    "Mouse": 500,
    "Keyboard": 1500,
    "Monitor": 12000
}

# 1. Print the price of the "Monitor"
print("Price of Monitor:", products["Monitor"])

# 2. Add a new product "Webcam" with price 3000
products["Webcam"] = 3000

# 3. Update the price of "Mouse" to 550
products["Mouse"] = 550

# 4. Print all product names and prices using a loop
print("\nUpdated Product List:")
for product, price in products.items():
    print(f"{product}: ₹{price}")
