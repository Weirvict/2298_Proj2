import pandas as pd
from faker import Faker
import random
import re


fake = Faker()

NUM_ROWS = 15000
ROGUE_PERCENT = 0.01
data = []

def random_ecommerce_site():
    suffixes = ["shop", "store", "mart", "market", "fashion", "tech", "deals"]
    company = fake.company().split()[0].lower()
    company = re.sub(r'[^a-z0-9]','',company)
    suffix = random.choice(suffixes)
    tld = random.choice([".com", ".net", ".co", ".io", ".in"])
    return f"www.{company}{suffix}{tld}"
website_pool = [random_ecommerce_site() for _ in range(200)]

for _ in range(NUM_ROWS):
    ecommerce_website_name = random.choices(website_pool)
    payment_txn_id = f"TXN{fake.unique.random_int(1000000,9999999)}"
    payment_txn_success = random.choices(["Y", "N"],weights=[0.9,0.1])[0]
    failure_reason = ""
    if payment_txn_success == "N":
        failure_reason = random.choice([
            "Card Declined", "Insufficient Funds","Expired Card","Invalid CVV","Network Error"
            ])
    
    data.append([ecommerce_website_name,payment_txn_id,payment_txn_success,failure_reason])

df = pd.DataFrame(data, columns=["ecommerce_website_name",
                                "payment_txn_id",
                                "payment_txn_success",
                                "failure_reason"])

for _ in range(int(NUM_ROWS * ROGUE_PERCENT)):
    idx = random.randint(0,len(df) - 1)
    col = random.choice(df.columns)
    if col == "ecommerce_website_name":
        df.at[idx, col] = random.choice(["",None, "Unknown"])
    elif col == "payment_txn_id":
        df.at[idx, col] = random.choice(["",None,"???","@#%","Unknown"])
    elif col =="payment_txn_success":
        df.at[idx, col] = random.choice(["",None,"Undefined", "N/A","?","$"])
    elif col =="failure_reason":
        df.at[idx, col] = random.choice(["",None,"None","Undefined", "N/A"])

df.to_csv("generated_data.csv",index=False)

