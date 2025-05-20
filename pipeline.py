import pandas as pd
import os
import clean_text

df = pd.read_csv("cnn_dailymail/test.csv")
print("ochub 1")
df["article_clean"] = df["article"].apply(clean_text.clean_text)
print("ochub 2")
df["highlights_clean"] = df["highlights"].apply(clean_text.clean_text)
print("ochub 3")
os.makedirs("data", exist_ok=True)
df.to_csv("data/cleaned_articles.csv", index=False)
print("✅ Cleaning done and saved.")
