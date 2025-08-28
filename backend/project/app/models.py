from django.db import models

# Create your models here.

class Status(models.Model):
    STATUS_CHOICES=[
        ('Connection Released','Connection Released'),
        ('Approved','Approved'),
        ('Pending','Pending'),
        ('Rejected','Rejected'),
    ]
    Status_Name=models.CharField(max_length=40,choices=STATUS_CHOICES)

    def __str__(self):
        return self.Status_Name
    
class Applicant(models.Model):
    GENDER_CHOICES = [
        ('Male', 'Male'),
        ('Female', 'Female'),
    ]
    OWNERSHIP_CHOICES = [
        ('INDIVIDUAL', 'INDIVIDUAL'),
        ('JOINT', 'JOINT'),
    ]
    GOVT_ID_CHOICES = [
        ('AADHAR', 'AADHAR'),
        ('VOTER_ID', 'VOTER_ID'),
        ('PAN', 'PAN'),
        ('PASSPORT', 'PASSPORT'),
    ]
    CATEGORY_CHOICES = [
        ('Residential', 'Residential'),
        ('Commercial', 'Commercial'),
    ]

    Applicant_Name=models.CharField(max_length=100)
    Gender=models.CharField(max_length=10,choices=GENDER_CHOICES)
    District=models.CharField(max_length=100)
    State=models.CharField(max_length=100)
    Pincode=models.IntegerField()
    Ownership=models.CharField(max_length=20,choices=OWNERSHIP_CHOICES)
    GovtID_Type=models.CharField(max_length=20,choices=GOVT_ID_CHOICES)
    ID_Number=models.CharField(max_length=100)
    Category=models.CharField(max_length=20,choices=CATEGORY_CHOICES)
    
    def __str__(self):
        return self.Applicant_Name
    

class Connection(models.Model):
    REVIEWER_COMMENTS_CHOICES = [
        ('Installation pending', 'Installation pending'),
        ('Documents verification in progress', 'Documents verification in progress'),
        ('Installation completed', 'Installation completed'),
        ('KYC failed', 'KYC failed'),
    ]
    Applicant=models.ForeignKey(Applicant, on_delete=models.CASCADE)
    Load_Applied=models.IntegerField()
    Date_of_Application=models.DateField()
    Date_of_Approval=models.DateField(null=True, blank=True)
    Modified_Date = models.DateField(null=True, blank=True)
    Status = models.ForeignKey(Status, on_delete=models.CASCADE)
    Reviewer_ID = models.IntegerField()
    Reviewer_Name = models.CharField(max_length=100)
    Reviewer_Comments = models.CharField(max_length=50, choices=REVIEWER_COMMENTS_CHOICES)

    def __str__(self):
        return f"Connection ID: {self.id} - Applicant: {self.Applicant}"
