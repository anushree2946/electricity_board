from django.contrib import admin
from .models import Applicant,Connection,Status
# Register your models here.

admin.site.register(Applicant)
admin.site.register(Connection)
admin.site.register(Status)