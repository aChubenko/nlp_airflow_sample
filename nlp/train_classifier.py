import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
import joblib

# 1. Загружаем размеченные данные (например, 200+ статей с полем 'text' и 'label')
df = pd.read_csv("data/train_labeled.csv")  # файл: text,label

# 2. Обучаем TF-IDF
vectorizer = TfidfVectorizer(max_features=5000)
X = vectorizer.fit_transform(df["text"])
y = df["label"]

# 3. Обучаем классификатор
clf = LogisticRegression(max_iter=1000)
clf.fit(X, y)

# 4. Сохраняем модель и векторизатор
joblib.dump(vectorizer, "models/tfidf_vectorizer.pkl")
joblib.dump(clf, "models/research_field_clf.pkl")

print("✅ Обучено и сохранено.")
