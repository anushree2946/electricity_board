# In backend/project/app/management/commands/load_data.py

import os
import pandas as pd
from datetime import datetime
from django.core.management.base import BaseCommand
from app.models import Applicant, Connection, Status

class Command(BaseCommand):
    help = 'Loads data from the electricity_board_case_study.csv file into the database'

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting data load process...")

        # Path to the CSV file, assuming it's in the root of your Django project (where manage.py is)
        # This path is relative to where manage.py is run.
        filepath = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'electricity_board_case_study.csv')

        try:
            df = pd.read_csv(filepath, encoding='latin-1')
            self.stdout.write(f"Successfully opened {filepath}")
        except FileNotFoundError:
            self.stderr.write(self.style.ERROR(f"CSV file not found at {filepath}"))
            return

        for index, row in df.iterrows():
            try:
                applicant, created = Applicant.objects.get_or_create(
                    ID_Number=row['ID_Number'],
                    defaults={
                        'Applicant_Name': row['Applicant_Name'],
                        'Gender': row['Gender'],
                        'District': row['District'],
                        'State': row['State'],
                        'Pincode': row['Pincode'],
                        'Ownership': row['Ownership'],
                        'GovtID_Type': row['GovtID_Type'],
                        'Category': row['Category']
                    }
                )

                status, created = Status.objects.get_or_create(Status_Name=row['Status'])

                date_of_application = datetime.strptime(row['Date_of_Application'], "%d-%m-%Y").strftime("%Y-%m-%d")

                date_of_approval = None
                if pd.notna(row['Date_of_Approval']):
                    date_of_approval = datetime.strptime(row['Date_of_Approval'], "%d-%m-%Y").strftime("%Y-%m-%d")

                modified_date = datetime.strptime(row['Modified_Date'], "%d-%m-%Y").strftime("%Y-%m-%d")

                Connection.objects.get_or_create(
                    Applicant=applicant,
                    ID=row['ID'],
                    defaults={
                        'Load_Applied': row['Load_Applied'],
                        'Date_of_Application': date_of_application,
                        'Date_of_Approval': date_of_approval,
                        'Modified_Date': modified_date,
                        'Status': status,
                        'Reviewer_ID': row['Reviewer_ID'],
                        'Reviewer_Name': row['Reviewer_Name'],
                        'Reviewer_Comments': row['Reviewer_Comments']
                    }
                )
            except Exception as e:
                self.stderr.write(self.style.ERROR(f"Error processing row {index}: {e}"))

        self.stdout.write(self.style.SUCCESS('Successfully loaded all data into the database.'))