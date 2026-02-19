import pandas as pd
from sqlalchemy import create_engine, text

# ---------- 1. Read CSV ----------
df = pd.read_csv(r"Titanic\Titanic-Dataset.csv")

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

# ---------- 4. Create Tables if Not Exists ----------
create_sql = """
CREATE TABLE IF NOT EXISTS passengers (
    PassengerId INT PRIMARY KEY,
    Name VARCHAR(255),
    Sex VARCHAR(10),
    Age FLOAT,
    Survived INT,
    Embarked VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS travel_details (
    PassengerId INT PRIMARY KEY,
    Pclass INT,
    SibSp INT,
    Parch INT
);

CREATE TABLE IF NOT EXISTS tickets (
    PassengerId INT PRIMARY KEY,
    Ticket VARCHAR(50),
    Fare FLOAT,
    Cabin VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS age (
    PassengerId INT PRIMARY KEY,
    Name VARCHAR(255),
    Age FLOAT
);

CREATE TABLE IF NOT EXISTS sex (
    PassengerId INT PRIMARY KEY,
    Name VARCHAR(255),
    Sex VARCHAR(10)
);
"""

with engine.connect() as conn:
    for statement in create_sql.split(";"):
        if statement.strip():
            conn.execute(text(statement))

    # ---------- 5. Truncate Tables ----------
    conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
    conn.execute(text("TRUNCATE TABLE tickets"))
    conn.execute(text("TRUNCATE TABLE travel_details"))
    conn.execute(text("TRUNCATE TABLE passengers"))
    conn.execute(text("TRUNCATE TABLE age"))
    conn.execute(text("TRUNCATE TABLE sex"))
    conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
    conn.commit()

# ---------- 6. Insert Fresh Data ----------
passengers_df.to_sql("passengers", engine, if_exists="append", index=False)
travel_details_df.to_sql("travel_details", engine, if_exists="append", index=False)
tickets_df.to_sql("tickets", engine, if_exists="append", index=False)
age_df.to_sql("age", engine, if_exists="append", index=False)
sex_df.to_sql("sex", engine, if_exists="append", index=False)

print("All tables created (if needed) and refreshed successfully!")
