import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple
import logging
import re
from pathlib import Path

from ..config import PROCESSED_DATA_DIR, FEATURE_PARAMS

logger = logging.getLogger(__name__)

class DataTransformer:
    """
    Handles data cleaning, transformation, and feature engineering.
    """
    
    def __init__(self):
        """Initialize the DataTransformer."""
        self.feature_params = FEATURE_PARAMS
    
    def clean_patient_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean patient data.
        
        Args:
            df: Raw patient data
            
        Returns:
            Cleaned patient data
        """
        if df.empty:
            return df
            
        df_clean = df.copy()
        
        # Standardize text fields
        text_columns = ['first_name', 'last_name', 'gender', 'address', 'email', 'insurance_provider']
        for col in text_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
        
        # Clean phone numbers
        if 'phone' in df_clean.columns:
            df_clean['phone'] = df_clean['phone'].astype(str).apply(self._clean_phone_number)
        
        # Clean email addresses
        if 'email' in df_clean.columns:
            df_clean['email'] = df_clean['email'].str.lower().str.strip()
            
        # Convert date fields to datetime
        date_columns = ['date_of_birth']
        for col in date_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        
        # Calculate age
        if 'date_of_birth' in df_clean.columns:
            df_clean['age'] = (pd.Timestamp.now() - df_clean['date_of_birth']).dt.days // 365
            df_clean['age_group'] = pd.cut(
                df_clean['age'],
                bins=[0, 18, 30, 45, 60, 75, 90, 120],
                labels=['0-18', '19-30', '31-45', '46-60', '61-75', '76-90', '90+'],
                right=False
            )
        
        logger.info("Successfully cleaned patient data")
        return df_clean
    
    def clean_admission_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean admission data.
        
        Args:
            df: Raw admission data
            
        Returns:
            Cleaned admission data
        """
        if df.empty:
            return df
            
        df_clean = df.copy()
        
        # Convert date fields to datetime
        date_columns = ['admission_date', 'discharge_date']
        for col in date_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        
        # Calculate length of stay
        if all(col in df_clean.columns for col in ['admission_date', 'discharge_date']):
            df_clean['length_of_stay'] = (df_clean['discharge_date'] - df_clean['admission_date']).dt.days + 1
            df_clean['length_of_stay'] = df_clean['length_of_stay'].clip(lower=0)  # Ensure non-negative
        
        # Clean categorical fields
        if 'admission_type' in df_clean.columns:
            df_clean['admission_type'] = df_clean['admission_type'].str.lower().str.strip()
            df_clean['admission_type'] = df_clean['admission_type'].replace({
                'er': 'emergency',
                'em': 'emergency',
                'elective': 'scheduled',
                'urgent': 'emergency'
            })
        
        if 'discharge_disposition' in df_clean.columns:
            df_clean['discharge_disposition'] = df_clean['discharge_disposition'].str.lower().str.strip()
        
        logger.info("Successfully cleaned admission data")
        return df_clean
    
    def clean_diagnosis_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean diagnosis data.
        
        Args:
            df: Raw diagnosis data
            
        Returns:
            Cleaned diagnosis data
        """
        if df.empty:
            return df
            
        df_clean = df.copy()
        
        # Clean ICD codes
        if 'icd_code' in df_clean.columns:
            df_clean['icd_code'] = df_clean['icd_code'].str.upper().str.strip()
            df_clean['icd_chapter'] = df_clean['icd_code'].apply(self._get_icd_chapter)
        
        # Clean descriptions
        if 'description' in df_clean.columns:
            df_clean['description'] = df_clean['description'].str.strip()
        
        logger.info("Successfully cleaned diagnosis data")
        return df_clean
    
    def clean_treatment_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean treatment data.
        
        Args:
            df: Raw treatment data
            
        Returns:
            Cleaned treatment data
        """
        if df.empty:
            return df
            
        df_clean = df.copy()
        
        # Convert date fields to datetime
        date_columns = ['start_date', 'end_date']
        for col in date_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        
        # Clean outcome field
        if 'outcome' in df_clean.columns:
            df_clean['outcome'] = df_clean['outcome'].str.lower().str.strip()
        
        logger.info("Successfully cleaned treatment data")
        return df_clean
    
    def clean_billing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean billing data.
        
        Args:
            df: Raw billing data
            
        Returns:
            Cleaned billing data
        """
        if df.empty:
            return df
            
        df_clean = df.copy()
        
        # Clean payment status
        if 'payment_status' in df_clean.columns:
            df_clean['payment_status'] = df_clean['payment_status'].str.lower().str.strip()
        
        # Ensure numeric fields are properly typed
        numeric_columns = ['total_charges', 'insurance_coverage', 'patient_responsibility']
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        logger.info("Successfully cleaned billing data")
        return df_clean
    
    def preprocess_for_ml(self, df: pd.DataFrame, target_column: str = None) -> Tuple[pd.DataFrame, Optional[pd.Series]]:
        """
        Preprocess data for machine learning.
        
        Args:
            df: Input DataFrame
            target_column: Name of the target column (if any)
            
        Returns:
            Tuple of (features, target) where features is a DataFrame and target is a Series or None
        """
        if df.empty:
            return df, None
            
        df_preprocessed = df.copy()
        
        # Handle target variable if specified
        target = None
        if target_column and target_column in df_preprocessed.columns:
            target = df_preprocessed[target_column]
            df_preprocessed = df_preprocessed.drop(columns=[target_column])
        
        # Handle missing values for numeric columns
        numeric_cols = df_preprocessed.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            if df_preprocessed[col].isnull().any():
                df_preprocessed[col].fillna(df_preprocessed[col].median(), inplace=True)
        
        # Handle categorical variables
        categorical_cols = df_preprocessed.select_dtypes(include=['object', 'category']).columns
        for col in categorical_cols:
            # Fill missing values with 'unknown'
            df_preprocessed[col] = df_preprocessed[col].fillna('unknown')
            
            # For high cardinality columns, group infrequent categories
            if df_preprocessed[col].nunique() > 20:  # Arbitrary threshold
                freq = df_preprocessed[col].value_counts(normalize=True)
                threshold = 0.01  # 1% threshold
                small_categories = freq[freq < threshold].index
                df_preprocessed[col] = df_preprocessed[col].replace(small_categories, 'other')
        
        # One-hot encode categorical variables
        if len(categorical_cols) > 0:
            df_preprocessed = pd.get_dummies(
                df_preprocessed, 
                columns=categorical_cols,
                drop_first=True,
                dtype=int
            )
        
        logger.info("Successfully preprocessed data for machine learning")
        return df_preprocessed, target
    
    def engineer_features_for_readmission(self, admissions_df: pd.DataFrame, patients_df: pd.DataFrame, 
                                        diagnoses_df: pd.DataFrame, treatments_df: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer features for readmission prediction.
        
        Args:
            admissions_df: DataFrame containing admission records
            patients_df: DataFrame containing patient information
            diagnoses_df: DataFrame containing diagnosis information
            treatments_df: DataFrame containing treatment information
            
        Returns:
            DataFrame with engineered features for readmission prediction
        """
        if admissions_df.empty:
            return pd.DataFrame()
            
        # Make copies to avoid modifying the original DataFrames
        admissions = admissions_df.copy()
        patients = patients_df.copy()
        diagnoses = diagnoses_df.copy()
        treatments = treatments_df.copy()
        
        # Ensure we have the necessary columns
        required_columns = ['patient_id', 'admission_date', 'discharge_date', 'admission_type', 'discharge_disposition']
        if not all(col in admissions.columns for col in required_columns):
            raise ValueError("Missing required columns in admissions data")
        
        # Sort admissions by patient and admission date
        admissions = admissions.sort_values(['patient_id', 'admission_date'])
        
        # Feature: Previous admissions count
        admissions['prev_admissions_count'] = admissions.groupby('patient_id').cumcount()
        
        # Feature: Days since last admission
        admissions['days_since_last_admission'] = admissions.groupby('patient_id')['admission_date'].diff().dt.days
        
        # Feature: Length of stay (already calculated in clean_admission_data)
        if 'length_of_stay' not in admissions.columns:
            admissions['length_of_stay'] = (admissions['discharge_date'] - admissions['admission_date']).dt.days + 1
        
        # Feature: Time of year (seasonality)
        admissions['admission_month'] = admissions['admission_date'].dt.month
        admissions['admission_dayofweek'] = admissions['admission_date'].dt.dayofweek
        admissions['is_weekend'] = admissions['admission_date'].dt.dayofweek.isin([5, 6]).astype(int)
        
        # Merge with patient data
        if not patients.empty and 'patient_id' in patients.columns:
            patients = self.clean_patient_data(patients)
            admissions = pd.merge(admissions, patients, on='patient_id', how='left')
        
        # Feature: Count of chronic conditions (from diagnoses)
        if not diagnoses.empty and 'patient_id' in diagnoses.columns and 'icd_code' in diagnoses.columns:
            # Identify chronic conditions (simplified for example)
            chronic_icd_prefixes = ['E11', 'I10', 'I25', 'E78', 'E66', 'J44', 'N18', 'F32']
            chronic_conditions = diagnoses[diagnoses['icd_code'].str.startswith(tuple(chronic_conditions), na=False)]
            chronic_counts = chronic_conditions.groupby('patient_id').size().reset_index(name='chronic_condition_count')
            admissions = pd.merge(admissions, chronic_counts, on='patient_id', how='left')
            admissions['chronic_condition_count'] = admissions['chronic_condition_count'].fillna(0)
        
        # Feature: Treatment complexity (simplified for example)
        if not treatments.empty and 'admission_id' in treatments.columns:
            treatment_counts = treatments.groupby('admission_id').size().reset_index(name='treatment_count')
            admissions = pd.merge(admissions, treatment_counts, on='admission_id', how='left')
            admissions['treatment_count'] = admissions['treatment_count'].fillna(0)
        
        # Drop columns that won't be used as features
        columns_to_drop = ['first_name', 'last_name', 'address', 'phone', 'email', 'insurance_policy_number']
        columns_to_drop = [col for col in columns_to_drop if col in admissions.columns]
        if columns_to_drop:
            admissions = admissions.drop(columns=columns_to_drop)
        
        logger.info("Successfully engineered features for readmission prediction")
        return admissions
    
    def _clean_phone_number(self, phone: str) -> str:
        """Clean and standardize phone numbers."""
        if pd.isna(phone) or phone == 'nan':
            return ''
            
        # Remove all non-digit characters
        cleaned = re.sub(r'\D', '', str(phone))
        
        # Handle international numbers (simplified)
        if len(cleaned) > 10 and cleaned.startswith('1'):
            cleaned = cleaned[1:]  # Remove US country code
        
        # Format as (XXX) XXX-XXXX
        if len(cleaned) == 10:
            return f"({cleaned[:3]}) {cleaned[3:6]}-{cleaned[6:]}"
        
        return phone  # Return original if format is unexpected
    
    def _get_icd_chapter(self, icd_code: str) -> str:
        """
        Map ICD code to chapter/category (simplified for example).
        In a real implementation, use a proper ICD code mapping.
        """
        if pd.isna(icd_code):
            return 'unknown'
            
        icd_code = str(icd_code).upper()
        
        # Very simplified mapping for example purposes
        if icd_code.startswith(('A', 'B')):
            return 'Infectious and Parasitic Diseases'
        elif icd_code.startswith(('C', 'D0', 'D1', 'D2', 'D3', 'D4')):
            return 'Neoplasms'
        elif icd_code.startswith(('E')):
            return 'Endocrine, Nutritional and Metabolic Diseases'
        elif icd_code.startswith(('F')):
            return 'Mental and Behavioral Disorders'
        elif icd_code.startswith(('G')):
            return 'Nervous System Diseases'
        elif icd_code.startswith(('I')):
            return 'Circulatory System Diseases'
        elif icd_code.startswith(('J')):
            return 'Respiratory Diseases'
        elif icd_code.startswith(('K')):
            return 'Digestive System Diseases'
        else:
            return 'Other'
