import pandas as pd
import numpy as np
import unittest
import sqlite3
import logging
from Data_Pipeline_Ingestion import df_combined_final, df_students, df_course, df_stud_jobs
from sqlalchemy import inspect
import os, time

logging.basicConfig(filename='logs/data_pipeline.log', level=logging.info, format='%(asctime)s:%(levelname)s:%(message)s')

def log_update(message):
    logging.info(message)

def log_error(message):
    logging.error(message)

class Test_Pipeline(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.df_combined_final = df_combined_final
        self.df_students = df_students
        self.df_course = df_course
        self.df_stud_jobs = df_stud_jobs

    def test_no_null_values(self):
        self.assertFalse(self.df_combined_final.isnull().values.any(), 'There are null values in the final table')

    def test_correct_number_of_rows(self):
        original_length = len(self.df_students)
        final_length = len(self.df_combined_final)
        self.assertEqual(original_length, final_length, 'Number of rows differ after the join')

    def test_schema_consistency(self):
        expected_schema = {"uuid", 'name', 'dob', 'sex', 'contact_info', 'job_id', 'job_category', 'avg_salary', 
                           'num_course_taken', 'time_spent_hrs', 'career_path_id', 'career_path_name', 'hours_to_complete'}
        final_schema = set(self.df_combined_final.columns)
        self.assertEqual(expected_schema, final_schema, 'Final table schema differs from expected')

if __name__ == '__main__':
    unittest.main()