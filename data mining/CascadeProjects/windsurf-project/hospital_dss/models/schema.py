from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

# Association tables for many-to-many relationships
patient_diagnosis = Table('patient_diagnosis', Base.metadata,
    Column('patient_id', Integer, ForeignKey('patients.id')),
    Column('diagnosis_id', Integer, ForeignKey('diagnoses.id'))
)

treatment_procedures = Table('treatment_procedures', Base.metadata,
    Column('treatment_id', Integer, ForeignKey('treatments.id')),
    Column('procedure_id', Integer, ForeignKey('procedures.id'))
)

class Patient(Base):
    __tablename__ = 'patients'
    
    id = Column(Integer, primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    date_of_birth = Column(Date, nullable=False)
    gender = Column(String(10))
    address = Column(String(200))
    phone = Column(String(20))
    email = Column(String(100))
    insurance_provider = Column(String(100))
    insurance_policy_number = Column(String(50))
    
    # Relationships
    admissions = relationship("Admission", back_populates="patient")
    diagnoses = relationship("Diagnosis", secondary=patient_diagnosis, back_populates="patients")

class Admission(Base):
    __tablename__ = 'admissions'
    
    id = Column(Integer, primary_key=True)
    patient_id = Column(Integer, ForeignKey('patients.id'), nullable=False)
    admission_date = Column(DateTime, default=datetime.utcnow)
    discharge_date = Column(DateTime)
    admission_type = Column(String(50))  # Emergency, Elective, Urgent
    discharge_disposition = Column(String(100))  # Home, Transfer, Expired, etc.
    readmission_status = Column(Boolean, default=False)
    
    # Relationships
    patient = relationship("Patient", back_populates="admissions")
    treatments = relationship("Treatment", back_populates="admission")
    billing = relationship("Billing", back_populates="admission", uselist=False)

class Diagnosis(Base):
    __tablename__ = 'diagnoses'
    
    id = Column(Integer, primary_key=True)
    icd_code = Column(String(20), unique=True, nullable=False)
    description = Column(String(200), nullable=False)
    
    # Relationships
    patients = relationship("Patient", secondary=patient_diagnosis, back_populates="diagnoses")
    treatments = relationship("Treatment", back_populates="diagnosis")

class Treatment(Base):
    __tablename__ = 'treatments'
    
    id = Column(Integer, primary_key=True)
    admission_id = Column(Integer, ForeignKey('admissions.id'), nullable=False)
    diagnosis_id = Column(Integer, ForeignKey('diagnoses.id'), nullable=False)
    start_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime)
    outcome = Column(String(100))  # Improved, Stable, Deteriorated, etc.
    
    # Relationships
    admission = relationship("Admission", back_populates="treatments")
    diagnosis = relationship("Diagnosis", back_populates="treatments")
    procedures = relationship("Procedure", secondary=treatment_procedures, back_populates="treatments")

class Procedure(Base):
    __tablename__ = 'procedures'
    
    id = Column(Integer, primary_key=True)
    cpt_code = Column(String(20), unique=True, nullable=False)
    description = Column(String(200), nullable=False)
    cost = Column(Float, nullable=False)
    
    # Relationships
    treatments = relationship("Treatment", secondary=treatment_procedures, back_populates="procedures")

class Billing(Base):
    __tablename__ = 'billing'
    
    id = Column(Integer, primary_key=True)
    admission_id = Column(Integer, ForeignKey('admissions.id'), nullable=False)
    total_charges = Column(Float, nullable=False)
    insurance_coverage = Column(Float, nullable=False)
    patient_responsibility = Column(Float, nullable=False)
    payment_status = Column(String(50))  # Paid, Pending, Denied, etc.
    
    # Relationships
    admission = relationship("Admission", back_populates="billing")
