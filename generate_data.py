#!/usr/bin/env python3
"""
generate_data.py - builds the two sample datasets used in the Hadoop demo.

  data/employees.csv   ~300,000 rows of made-up employee records (HDFS demo + streaming bonus)
  data/sample_text.txt ~60,000 lines of text about data engineering (WordCount demo)

Only the Python standard library is used, so there is nothing to pip install.
A fixed random seed means everyone gets the same files (and the same counts).
"""
import csv
import os
import random

random.seed(42)
os.makedirs("data", exist_ok=True)

# ---------------------------------------------------------------- employees.csv
DEPARTMENTS = {
    "Sales":            ["Sales Executive", "Sales Representative", "Account Manager"],
    "Research":         ["Research Scientist", "Lab Technician", "Research Director"],
    "Engineering":      ["Software Engineer", "Data Engineer", "DevOps Engineer", "QA Engineer"],
    "Human Resources":  ["HR Executive", "Recruiter"],
    "Finance":          ["Financial Analyst", "Accountant"],
    "Marketing":        ["Marketing Specialist", "Content Strategist"],
}
DEPT_WEIGHTS = [26, 14, 30, 6, 12, 12]
CITIES = ["Bengaluru", "Hyderabad", "Pune", "Chennai", "Mumbai", "Delhi", "Kolkata", "Ahmedabad"]

with open("data/employees.csv", "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["employee_id", "age", "gender", "department", "job_role", "city",
                "monthly_income", "years_at_company", "job_satisfaction",
                "overtime", "attrition"])
    for i in range(1, 300_001):
        dept = random.choices(list(DEPARTMENTS), weights=DEPT_WEIGHTS)[0]
        role = random.choice(DEPARTMENTS[dept])
        age = random.randint(21, 60)
        years = min(random.randint(0, 25), age - 20)
        income = int(random.gauss(55000, 18000) + years * 1500)
        income = max(income, 18000)
        overtime = random.choices(["Yes", "No"], weights=[28, 72])[0]
        satisfaction = random.randint(1, 4)
        # attrition is a bit more likely with overtime and low satisfaction
        p = 0.08 + (0.10 if overtime == "Yes" else 0) + (0.07 if satisfaction == 1 else 0)
        attrition = "Yes" if random.random() < p else "No"
        w.writerow([i, age, random.choice(["Male", "Female"]), dept, role,
                    random.choice(CITIES), income, years, satisfaction,
                    overtime, attrition])

# -------------------------------------------------------------- sample_text.txt
WORDS = """hadoop hdfs yarn mapreduce data cluster node block replica namenode datanode
job task map reduce shuffle sort key value record file directory batch pipeline
storage compute scale distributed parallel fault tolerant commodity server disk
network input output split partition counter log schema table query analytics
spark hive kafka warehouse lake ingest transform load stream process engineer
the a of to and in is for with on that by from as at this are be or it""".split()
# first words get picked far more often -> a nice skewed word-count result
weights = [1 / (rank + 3) for rank in range(len(WORDS))]

with open("data/sample_text.txt", "w", newline="\n") as f:
    for _ in range(60_000):
        n = random.randint(8, 18)
        f.write(" ".join(random.choices(WORDS, weights=weights, k=n)) + "\n")

print("Done: data/employees.csv and data/sample_text.txt")
