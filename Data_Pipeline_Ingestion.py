import pandas as pd
import numpy as np
import sqlite3
import csv
from sqlalchemy import create_engine, ForeignKey, Column, String, Integer, CHAR, DATE, FLOAT
from sqlalchemy.orm import sessionmaker, declarative_base
import os

#-------Step 1-------
#Importing Databases
connection = sqlite3.connect("C:\\Users\\garre\\Downloads\\subscriber-pipeline-starter-kit\\subscriber-pipeline-starter-kit\\dev\\cademycode.db")
curs = connection.cursor()

#-------Step 2-------
#Show What Tables are Present
curs.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = curs.fetchall()
print("Tables in the database:", tables)

#Get Some Info on Each Table
df_students = pd.read_sql_query("SELECT * FROM cademycode_students", connection)
df_course = pd.read_sql_query("SELECT * FROM cademycode_courses", connection)
df_stud_jobs = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", connection)

print("------INFO FOR STUDENTS DB------")
print(df_students.info())
print("------INFO FOR COURSE DB------")
print(df_course.info())
print("------INFO FOR STUDENT JOBS DB------")
print(df_stud_jobs.info())


#Check to See if any Missing Data
print("------NUMBER OF N/A VALUES PER TABLE------")
print(df_students.isna().sum())
print(df_course.isna().sum())
print(df_stud_jobs.isna().sum())

#Count Unique Valus
print("------UNIQUE VALUES OF STUDENT TABLE------")
print(df_students.nunique())
print("------UNIQUE VALUES OF COURSE TABLE------")
print(df_course.nunique())
print("------UNIQUE VALUES OF STUDENT JOBS TABLE------")
print(df_stud_jobs.nunique())

#Summary Statistics for Student Database
print('------STUDENT STATISTICS------')
df_students['time_spent_hrs'] = pd.to_numeric(df_students['time_spent_hrs'], errors='coerce')
print(df_students[['time_spent_hrs']].describe())
df_students['num_course_taken'] = pd.to_numeric(df_students['num_course_taken'], errors='coerce')
print(df_students[['num_course_taken']].describe())

#Summary Statistics for Course DB
print('------COURSE STATISTICS------')
print(df_course[['hours_to_complete']].describe())

#Summary Statistics for Student Jobs 
print('------STUDENT JOB STATISITCS------')
print(df_stud_jobs[['avg_salary']].describe())

#Combine into the Final Database
df_students['job_id'] = pd.to_numeric(df_students['job_id'], errors='coerce')

df_students = df_students.rename(columns={'current_career_path_id': 'career_path_id'})
df_students['career_path_id'] = pd.to_numeric(df_students['career_path_id'], errors='coerce')

df_combined_final = pd.merge(df_students, df_stud_jobs, how='left', on='job_id')
df_combined_final = pd.merge(df_combined_final, df_course, how='left', on='career_path_id')

df_combined_final = df_combined_final.reindex(columns=["uuid", 'name', 'dob', 'sex', 'contact_info', 'job_id', 'job_category', 'avg_salary', 'num_course_taken', 'time_spent_hrs', 
                                                       'career_path_id', 'career_path_name', 'hours_to_complete'])
df_combined_final = df_combined_final.drop_duplicates(subset='uuid')
df_combined_final = df_combined_final.fillna(value='N/A')
print(df_combined_final.head())
print(df_combined_final.info())

#-------Step 3-------
#Create the SQLite Table for Subscribers
Base = declarative_base()

class Subscriber(Base):
    __tablename__ = 'Subscribers'

    uuid= Column('uuid', Integer, primary_key=True)
    name = Column('name', String)
    dob = Column('dob', String)
    sex = Column('sex', String)
    contact_info = Column('contact_info', String)
    job_id = Column('job_id', String)
    job_category = Column('job_category', String)
    avg_salary = Column('avg_salary', String)
    num_course_taken = Column('num_course_taken', Integer)
    time_spent_hrs = Column('time_spent_hrs', Integer)
    career_path_id = Column('career_path_id', Integer)
    career_path_name = Column('career_path_name', Integer)
    hours_to_complete = Column('hours_to_complete', Integer)

engine = create_engine('sqlite:///cleaned_db.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

#Insert Data
df_combined_final.to_sql('Subscribers', con=engine, if_exists='replace', index=False)