import pandas as pd
from sqlalchemy import create_engine, text

# ---------- 1. Read CSV ----------
df = pd.read_csv("Titanic\Titanic-Dataset.csv")

# ---------- 2. Split DataFrames ----------
passengers_df = df[[
    "PassengerId", "Name", "Sex",
    "Age", "Survived", "Embarked"
]]

travel_details_df = df[[
    "PassengerId", "Pclass",
    "SibSp", "Parch"
]]

tickets_df = df[[
    "PassengerId", "Ticket",
    "Fare", "Cabin"
]]

age_df = df[[
    "PassengerId", "Name", "Age"
]]

sex_df = df[[
    "PassengerId", "Name", "Sex"
]]

# ---------- 3. MySQL Connection ----------
engine = create_engine(
    "mysql+pymysql://root:qwer@localhost/titanic"
)

# ---------- 4. Truncate Tables First ----------
with engine.connect() as conn:
    conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
    conn.execute(text("TRUNCATE TABLE tickets"))
    conn.execute(text("TRUNCATE TABLE travel_details"))
    conn.execute(text("TRUNCATE TABLE passengers"))
    conn.execute(text("TRUNCATE TABLE Age"))
    conn.execute(text("TRUNCATE TABLE Sex"))
    conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
    conn.commit()

# ---------- 5. Insert Fresh Data ----------
passengers_df.to_sql("passengers", engine, if_exists="append", index=False)
travel_details_df.to_sql("travel_details", engine, if_exists="append", index=False)
tickets_df.to_sql("tickets", engine, if_exists="append", index=False)
age_df.to_sql("Age", engine, if_exists="append", index=False)

print(" All tables refreshed successfully without duplicates!")

print(" wow nice ")