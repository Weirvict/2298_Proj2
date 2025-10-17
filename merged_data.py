import pandas as pd

df1 = pd.read_csv(r"\Users\kevin\.vscode\Python_Examples\Python_activity_2\project_2\fake_orders.csv")
df2 = pd.read_csv(r"\Users\kevin\.vscode\Python_Examples\Python_activity_2\project_2\E_Commerce_data_Victoria.csv",on_bad_lines="skip")
df3 = pd.read_csv(r"\Users\kevin\.vscode\Python_Examples\Python_activity_2\project_2\generated_data.csv")



 
combined_df = pd.concat([ df1,df2, df3], axis=1)

combined_df.to_csv("final_ecommerce_data.csv", index=False)

print(len(combined_df))

#combined_df

