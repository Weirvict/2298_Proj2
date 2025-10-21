import random
from faker import Faker
import faker_commerce
# from faker.providers import commerce
import pandas as pd

fake = Faker()
fake.add_provider(faker_commerce.Provider)

num_rows = 15000
rouge_percent = 0.02  # 2% rogue data

def product_cats():
  products = []
  num_products = 100
  
  for pid in range(1, num_products+1):
    fake.seed_instance(pid)
    name = fake.ecommerce_name()
    fake.seed_instance(name)
    category = fake.ecommerce_category()
    price = round(fake.pyfloat(left_digits=4, right_digits=2, positive=True, min_value=5, max_value=2000), 2)
    products.append({
        "product_id": str(pid),
        "product_name": name,
        "product_category": category,
        "price": price
    })
  
  product = random.choice(products)
  return product


def generate():
  data = []

  for i in range(num_rows):
    order_id = i + 1
    customer_id = random.randint(1, num_rows)
    fake.seed_instance(customer_id)
    customer_name = fake.name()
    payment_type = random.choice(['Credit Card', 'Debit Card', 'UPI', 'Bank Transfer', 'Wallet'])
    qty = random.randint(1, 100)

    product = product_cats()
    product_id = product["product_id"]
    product_name = product["product_name"]
    product_category = product["product_category"]
    price = product["price"]

    data.append([int(order_id), customer_id, customer_name, product_id, product_name, product_category, payment_type, qty, price])
  
  return data

def main():
  gen_data = generate()
  df = pd.DataFrame(gen_data, columns=["order_id", "customer_id", "customer_name", "product_id", "product_name", "product_category", "payment_type", "qty", "price"])

  for _ in range(int(num_rows * rouge_percent)):
    idx = random.randint(0,len(df) - 1)
    col = random.choice(df.columns)
    if col == "order_id":
      df.at[idx, col] = random.choice([-1])
    elif col == "customer_id":
      df.at[idx, col] = random.choice([-1])
    elif col =="customer_name":
      df.at[idx, col] = random.choice(["",None,"Undefined", "N/A","?"])
    elif col =="product_id":
      df.at[idx, col] = random.choice([None])
    elif col == "product_name":
      df.at[idx, col] = random.choice(["",None,"???","@#%","Unknown"])
    elif col =="product_category":
      df.at[idx, col] = random.choice(["",None,"Undefined","N/A","?","$"])
    elif col =="payment_type":
      df.at[idx, col] = random.choice(["",None,"None","Undefined", "N/A"])
    elif col =="price":
      df.at[idx, col] = random.choice([0,None,-1])

  df.to_csv("fake_orders.csv", index=False)

if __name__ == "__main__":
  main()

