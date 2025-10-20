import pandas as pd

df1 = pd.read_csv(r"fake_orders.csv")
df2 = pd.read_csv(r"E_Commerce_data_Victoria.csv")
df3 = pd.read_csv(r"generated_data.csv")



 
combined_df = pd.concat([ df1,df2, df3], axis=1)

combined_df.to_csv("final_ecommerce_data.csv", index=False)

print(len(combined_df))

#combined_df

