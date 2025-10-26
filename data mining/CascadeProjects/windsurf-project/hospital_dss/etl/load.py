import pandas as pd
import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Tuple
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, Date, DateTime, Boolean, ForeignKey, inspect
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.exc import SQLAlchemyError
import json

from ..config import DB_CONFIG, PROCESSED_DATA_DIR
from ..models.schema import Base, Patient, Admission, Diagnosis, Treatment, Procedure, Billing

logger = logging.getLogger(__name__)

class DataLoader:
    """
    Handles loading data to various destinations including databases and files.
    """
    
    def __init__(self, connection_string: str = None):
        """
        Initialize the DataLoader.
        
        Args:
            connection_string: SQLAlchemy connection string. If None, uses DB_CONFIG.
        """
        self.connection_string = connection_string or (
            f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        self.engine = None
        self.Session = None
    
    def connect(self):
        """Establish a database connection."""
        try:
            self.engine = create_engine(self.connection_string)
            self.Session = sessionmaker(bind=self.engine)
            logger.info("Successfully connected to the database")
            return True
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            return False
    
    def create_tables(self):
        """Create database tables based on the SQLAlchemy models."""
        if self.engine is None:
            self.connect()
        
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Successfully created database tables")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Error creating database tables: {str(e)}")
            return False
    
    def load_to_database(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append', 
                        index: bool = False, chunksize: int = 1000) -> bool:
        """
        Load a DataFrame to a database table.
        
        Args:
            df: DataFrame to load
            table_name: Name of the target table
            if_exists: What to do if table exists: 'fail', 'replace', or 'append'
            index: Whether to write the DataFrame index as a column
            chunksize: Number of rows to write at a time
            
        Returns:
            True if successful, False otherwise
        """
        if df.empty:
            logger.warning("Empty DataFrame, nothing to load")
            return False
            
        if self.engine is None:
            self.connect()
        
        try:
            with self.engine.begin() as connection:
                df.to_sql(
                    name=table_name,
                    con=connection,
                    if_exists=if_exists,
                    index=index,
                    chunksize=chunksize,
                    method='multi'
                )
            logger.info(f"Successfully loaded {len(df)} rows to {table_name}")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Error loading data to {table_name}: {str(e)}")
            return False
    
    def save_to_csv(self, df: pd.DataFrame, file_name: str, index: bool = False) -> bool:
        """
        Save a DataFrame to a CSV file.
        
        Args:
            df: DataFrame to save
            file_name: Name of the output file (without extension)
            index: Whether to write row names
            
        Returns:
            Path to the saved file if successful, None otherwise
        """
        if df.empty:
            logger.warning("Empty DataFrame, nothing to save")
            return None
            
        try:
            # Ensure the processed data directory exists
            os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
            
            # Add .csv extension if not present
            if not file_name.lower().endswith('.csv'):
                file_name += '.csv'
                
            file_path = PROCESSED_DATA_DIR / file_name
            df.to_csv(file_path, index=index)
            logger.info(f"Successfully saved data to {file_path}")
            return str(file_path)
        except Exception as e:
            logger.error(f"Error saving data to {file_name}: {str(e)}")
            return None
    
    def save_to_parquet(self, df: pd.DataFrame, file_name: str) -> Optional[str]:
        """
        Save a DataFrame to a Parquet file.
        
        Args:
            df: DataFrame to save
            file_name: Name of the output file (without extension)
            
        Returns:
            Path to the saved file if successful, None otherwise
        """
        if df.empty:
            logger.warning("Empty DataFrame, nothing to save")
            return None
            
        try:
            # Ensure the processed data directory exists
            os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
            
            # Add .parquet extension if not present
            if not file_name.lower().endswith(('.parquet', '.parq')):
                file_name += '.parquet'
                
            file_path = PROCESSED_DATA_DIR / file_name
            df.to_parquet(file_path, index=False)
            logger.info(f"Successfully saved data to {file_path}")
            return str(file_path)
        except Exception as e:
            logger.error(f"Error saving data to {file_name}: {str(e)}")
            return None
    
    def load_patient(self, patient_data: Dict[str, Any]) -> Optional[int]:
        """
        Load a single patient record into the database.
        
        Args:
            patient_data: Dictionary containing patient data
            
        Returns:
            ID of the created/updated patient record, or None if failed
        """
        if not patient_data:
            logger.warning("Empty patient data, nothing to load")
            return None
            
        if self.engine is None:
            self.connect()
        
        session = self.Session()
        try:
            # Check if patient already exists
            patient = session.query(Patient).filter_by(
                first_name=patient_data.get('first_name'),
                last_name=patient_data.get('last_name'),
                date_of_birth=patient_data.get('date_of_birth')
            ).first()
            
            if patient:
                # Update existing patient
                for key, value in patient_data.items():
                    if hasattr(patient, key):
                        setattr(patient, key, value)
                logger.info(f"Updated patient record: {patient.id}")
            else:
                # Create new patient
                patient = Patient(**patient_data)
                session.add(patient)
                logger.info("Created new patient record")
            
            session.commit()
            return patient.id
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error loading patient data: {str(e)}")
            return None
        finally:
            session.close()
    
    def load_admission(self, admission_data: Dict[str, Any]) -> Optional[int]:
        """
        Load a single admission record into the database.
        
        Args:
            admission_data: Dictionary containing admission data
            
        Returns:
            ID of the created/updated admission record, or None if failed
        """
        if not admission_data:
            logger.warning("Empty admission data, nothing to load")
            return None
            
        if 'patient_id' not in admission_data:
            logger.error("Missing required field: patient_id")
            return None
            
        if self.engine is None:
            self.connect()
        
        session = self.Session()
        try:
            # Check if admission already exists
            admission = session.query(Admission).filter_by(
                patient_id=admission_data.get('patient_id'),
                admission_date=admission_data.get('admission_date')
            ).first()
            
            if admission:
                # Update existing admission
                for key, value in admission_data.items():
                    if hasattr(admission, key):
                        setattr(admission, key, value)
                logger.info(f"Updated admission record: {admission.id}")
            else:
                # Create new admission
                admission = Admission(**admission_data)
                session.add(admission)
                logger.info("Created new admission record")
            
            session.commit()
            return admission.id
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error loading admission data: {str(e)}")
            return None
        finally:
            session.close()
    
    def load_diagnosis(self, diagnosis_data: Dict[str, Any]) -> Optional[int]:
        """
        Load a single diagnosis record into the database.
        
        Args:
            diagnosis_data: Dictionary containing diagnosis data
            
        Returns:
            ID of the created/updated diagnosis record, or None if failed
        """
        if not diagnosis_data:
            logger.warning("Empty diagnosis data, nothing to load")
            return None
            
        if 'icd_code' not in diagnosis_data:
            logger.error("Missing required field: icd_code")
            return None
            
        if self.engine is None:
            self.connect()
        
        session = self.Session()
        try:
            # Check if diagnosis already exists
            diagnosis = session.query(Diagnosis).filter_by(
                icd_code=diagnosis_data.get('icd_code')
            ).first()
            
            if diagnosis:
                # Update existing diagnosis
                for key, value in diagnosis_data.items():
                    if hasattr(diagnosis, key):
                        setattr(diagnosis, key, value)
                logger.info(f"Updated diagnosis record: {diagnosis.id}")
            else:
                # Create new diagnosis
                diagnosis = Diagnosis(**diagnosis_data)
                session.add(diagnosis)
                logger.info("Created new diagnosis record")
            
            session.commit()
            return diagnosis.id
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error loading diagnosis data: {str(e)}")
            return None
        finally:
            session.close()
    
    def load_treatment(self, treatment_data: Dict[str, Any]) -> Optional[int]:
        """
        Load a single treatment record into the database.
        
        Args:
            treatment_data: Dictionary containing treatment data
            
        Returns:
            ID of the created/updated treatment record, or None if failed
        """
        if not treatment_data:
            logger.warning("Empty treatment data, nothing to load")
            return None
            
        if 'admission_id' not in treatment_data or 'diagnosis_id' not in treatment_data:
            logger.error("Missing required fields: admission_id and diagnosis_id are required")
            return None
            
        if self.engine is None:
            self.connect()
        
        session = self.Session()
        try:
            # Check if treatment already exists
            treatment = session.query(Treatment).filter_by(
                admission_id=treatment_data.get('admission_id'),
                diagnosis_id=treatment_data.get('diagnosis_id'),
                start_date=treatment_data.get('start_date')
            ).first()
            
            if treatment:
                # Update existing treatment
                for key, value in treatment_data.items():
                    if hasattr(treatment, key):
                        setattr(treatment, key, value)
                logger.info(f"Updated treatment record: {treatment.id}")
            else:
                # Create new treatment
                treatment = Treatment(**treatment_data)
                session.add(treatment)
                logger.info("Created new treatment record")
            
            session.commit()
            return treatment.id
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error loading treatment data: {str(e)}")
            return None
        finally:
            session.close()
    
    def load_billing(self, billing_data: Dict[str, Any]) -> Optional[int]:
        """
        Load a single billing record into the database.
        
        Args:
            billing_data: Dictionary containing billing data
            
        Returns:
            ID of the created/updated billing record, or None if failed
        """
        if not billing_data:
            logger.warning("Empty billing data, nothing to load")
            return None
            
        if 'admission_id' not in billing_data:
            logger.error("Missing required field: admission_id")
            return None
            
        if self.engine is None:
            self.connect()
        
        session = self.Session()
        try:
            # Check if billing record already exists
            billing = session.query(Billing).filter_by(
                admission_id=billing_data.get('admission_id')
            ).first()
            
            if billing:
                # Update existing billing record
                for key, value in billing_data.items():
                    if hasattr(billing, key):
                        setattr(billing, key, value)
                logger.info(f"Updated billing record: {billing.id}")
            else:
                # Create new billing record
                billing = Billing(**billing_data)
                session.add(billing)
                logger.info("Created new billing record")
            
            session.commit()
            return billing.id
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error loading billing data: {str(e)}")
            return None
        finally:
            session.close()
    
    def link_patient_diagnosis(self, patient_id: int, diagnosis_id: int) -> bool:
        """
        Link a patient to a diagnosis in the many-to-many relationship.
        
        Args:
            patient_id: ID of the patient
            diagnosis_id: ID of the diagnosis
            
        Returns:
            True if successful, False otherwise
        """
        if self.engine is None:
            self.connect()
        
        session = self.Session()
        try:
            # Check if the link already exists
            exists = session.execute(
                "SELECT 1 FROM patient_diagnosis WHERE patient_id = :patient_id AND diagnosis_id = :diagnosis_id",
                {'patient_id': patient_id, 'diagnosis_id': diagnosis_id}
            ).scalar()
            
            if not exists:
                # Create the link
                session.execute(
                    "INSERT INTO patient_diagnosis (patient_id, diagnosis_id) VALUES (:patient_id, :diagnosis_id)",
                    {'patient_id': patient_id, 'diagnosis_id': diagnosis_id}
                )
                session.commit()
                logger.info(f"Linked patient {patient_id} to diagnosis {diagnosis_id}")
            
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error linking patient to diagnosis: {str(e)}")
            return False
        finally:
            session.close()
    
    def link_treatment_procedure(self, treatment_id: int, procedure_id: int) -> bool:
        """
        Link a treatment to a procedure in the many-to-many relationship.
        
        Args:
            treatment_id: ID of the treatment
            procedure_id: ID of the procedure
            
        Returns:
            True if successful, False otherwise
        """
        if self.engine is None:
            self.connect()
        
        session = self.Session()
        try:
            # Check if the link already exists
            exists = session.execute(
                "SELECT 1 FROM treatment_procedures WHERE treatment_id = :treatment_id AND procedure_id = :procedure_id",
                {'treatment_id': treatment_id, 'procedure_id': procedure_id}
            ).scalar()
            
            if not exists:
                # Create the link
                session.execute(
                    "INSERT INTO treatment_procedures (treatment_id, procedure_id) VALUES (:treatment_id, :procedure_id)",
                    {'treatment_id': treatment_id, 'procedure_id': procedure_id}
                )
                session.commit()
                logger.info(f"Linked treatment {treatment_id} to procedure {procedure_id}")
            
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error linking treatment to procedure: {str(e)}")
            return False
        finally:
            session.close()
