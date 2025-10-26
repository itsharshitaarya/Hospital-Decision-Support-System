import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging
from typing import Dict, Any, Optional, List, Union
import json

from ..config import DB_CONFIG, RAW_DATA_DIR, PROCESSED_DATA_DIR
from ..models.schema import Base, Patient, Admission, Diagnosis, Treatment, Procedure, Billing

logger = logging.getLogger(__name__)

class DataExtractor:
    """
    Handles data extraction from various sources including CSV, Excel, and SQL databases.
    """
    
    def __init__(self, data_dir: str = None):
        """
        Initialize the DataExtractor with a data directory.
        
        Args:
            data_dir: Directory containing raw data files
        """
        self.data_dir = Path(data_dir) if data_dir else RAW_DATA_DIR
        self.engine = None
        self.Session = None
    
    def connect_to_db(self, connection_string: str = None):
        """
        Establish a database connection.
        
        Args:
            connection_string: SQLAlchemy connection string. If None, uses DB_CONFIG.
        """
        if connection_string is None:
            connection_string = (
                f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
        
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        logger.info("Successfully connected to the database")
    
    def extract_from_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from a CSV file.
        
        Args:
            file_path: Path to the CSV file
            **kwargs: Additional arguments to pass to pandas.read_csv()
            
        Returns:
            DataFrame containing the extracted data
        """
        try:
            file_path = self.data_dir / file_path
            df = pd.read_csv(file_path, **kwargs)
            logger.info(f"Successfully extracted data from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error extracting data from {file_path}: {str(e)}")
            raise
    
    def extract_from_excel(self, file_path: str, sheet_name: str = 0, **kwargs) -> pd.DataFrame:
        """
        Extract data from an Excel file.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Name or index of the sheet to extract
            **kwargs: Additional arguments to pass to pandas.read_excel()
            
        Returns:
            DataFrame containing the extracted data
        """
        try:
            file_path = self.data_dir / file_path
            df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
            logger.info(f"Successfully extracted data from {file_path}, sheet: {sheet_name}")
            return df
        except Exception as e:
            logger.error(f"Error extracting data from {file_path}: {str(e)}")
            raise
    
    def extract_from_sql(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Extract data from the database using a SQL query.
        
        Args:
            query: SQL query to execute
            params: Parameters to pass to the query
            
        Returns:
            DataFrame containing the query results
        """
        if self.engine is None:
            self.connect_to_db()
        
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql_query(query, connection, params=params)
            logger.info("Successfully executed SQL query")
            return df
        except Exception as e:
            logger.error(f"Error executing SQL query: {str(e)}")
            raise
    
    def extract_from_api(self, url: str, params: Optional[Dict] = None, headers: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Extract data from a REST API.
        
        Args:
            url: API endpoint URL
            params: Query parameters
            headers: Request headers
            
        Returns:
            Dictionary containing the API response
        """
        import requests
        
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            logger.info(f"Successfully retrieved data from {url}")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error retrieving data from API: {str(e)}")
            raise
    
    def extract_all_patients(self) -> pd.DataFrame:
        """Extract all patient records from the database."""
        query = """
        SELECT * FROM patients
        """
        return self.extract_from_sql(query)
    
    def extract_patient_admissions(self, patient_id: int = None) -> pd.DataFrame:
        """
        Extract admission records for a specific patient or all patients.
        
        Args:
            patient_id: Optional patient ID to filter by
            
        Returns:
            DataFrame containing admission records
        """
        query = """
        SELECT a.*, p.first_name, p.last_name, p.date_of_birth
        FROM admissions a
        JOIN patients p ON a.patient_id = p.id
        """
        
        if patient_id is not None:
            query += " WHERE a.patient_id = :patient_id"
            return self.extract_from_sql(query, {'patient_id': patient_id})
        
        return self.extract_from_sql(query)
    
    def extract_readmission_data(self, readmission_window_days: int = 30) -> pd.DataFrame:
        """
        Extract data for readmission analysis.
        
        Args:
            readmission_window_days: Number of days to consider for readmission
            
        Returns:
            DataFrame with readmission data
        """
        query = """
        WITH ranked_admissions AS (
            SELECT 
                patient_id,
                admission_date,
                discharge_date,
                LEAD(admission_date) OVER (PARTITION BY patient_id ORDER BY admission_date) AS next_admission_date,
                discharge_disposition
            FROM admissions
        )
        SELECT 
            ra.*,
            p.first_name,
            p.last_name,
            p.date_of_birth,
            p.gender,
            CASE 
                WHEN ra.next_admission_date IS NOT NULL 
                     AND ra.next_admission_date <= ra.discharge_date + INTERVAL ':days days' 
                THEN TRUE 
                ELSE FALSE 
            END AS was_readmitted,
            EXTRACT(DAY FROM (ra.next_admission_date - ra.discharge_date)) AS days_to_readmission
        FROM ranked_admissions ra
        JOIN patients p ON ra.patient_id = p.id
        WHERE ra.discharge_date IS NOT NULL
        ORDER BY ra.patient_id, ra.admission_date
        """
        
        return self.extract_from_sql(query, {'days': readmission_window_days})
