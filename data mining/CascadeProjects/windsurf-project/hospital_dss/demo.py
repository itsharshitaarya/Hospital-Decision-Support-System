#!/usr/bin/env python3
"""
Hospital Decision Support System - Demonstration Script

This script demonstrates how to use the Hospital Decision Support System
to process hospital data, analyze readmission rates, and generate insights.
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.append(str(Path(__file__).parent))

# Import the ETL pipeline
from etl.pipeline import ETLPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('hospital_dss.log')
    ]
)
logger = logging.getLogger(__name__)

# Set up visualization style
plt.style.use('seaborn')
sns.set_palette('colorblind')
plt.rcParams['figure.figsize'] = (12, 6)

def setup_environment():
    """Set up the environment and verify dependencies."""
    logger.info("Setting up environment...")
    
    # Load environment variables
    load_dotenv()
    
    # Verify required directories exist
    data_dir = Path('data')
    raw_dir = data_dir / 'raw'
    processed_dir = data_dir / 'processed'
    
    for directory in [data_dir, raw_dir, processed_dir]:
        directory.mkdir(exist_ok=True, parents=True)
    
    logger.info("Environment setup complete")

def generate_sample_data():
    """Generate sample hospital data for demonstration purposes."""
    logger.info("Generating sample data...")
    
    np.random.seed(42)
    num_patients = 1000
    
    # Generate patient data
    patients = pd.DataFrame({
        'patient_id': range(1, num_patients + 1),
        'first_name': [f'Patient_{i}' for i in range(1, num_patients + 1)],
        'last_name': ['Doe'] * num_patients,
        'date_of_birth': pd.date_range('1930-01-01', '2020-12-31', periods=num_patients),
        'gender': np.random.choice(['M', 'F'], size=num_patients, p=[0.48, 0.52]),
        'address': ['123 Main St'] * num_patients,
        'phone': [f'555-{np.random.randint(100, 1000)}-{np.random.randint(1000, 10000)}' for _ in range(num_patients)],
        'email': [f'patient_{i}@example.com' for i in range(1, num_patients + 1)],
        'insurance_provider': np.random.choice(['Medicare', 'Medicaid', 'Private', 'Self-pay', 'Other'], 
                                             size=num_patients, 
                                             p=[0.4, 0.3, 0.2, 0.05, 0.05]),
        'insurance_policy_number': [f'POL-{i:06d}' for i in range(1, num_patients + 1)]
    })
    
    # Generate admission data
    admissions = []
    admission_id = 1
    
    for patient_id in range(1, num_patients + 1):
        # Each patient has 1-5 admissions
        num_admissions = np.random.choice([1, 2, 3, 4, 5], p=[0.6, 0.2, 0.1, 0.07, 0.03])
        
        for _ in range(num_admissions):
            admission_date = pd.Timestamp('2020-01-01') + pd.Timedelta(days=np.random.randint(0, 365))
            length_of_stay = np.random.randint(1, 30)  # 1-29 days
            discharge_date = admission_date + pd.Timedelta(days=length_of_stay)
            
            admission_type = np.random.choice(
                ['emergency', 'urgent', 'elective'],
                p=[0.5, 0.3, 0.2]
            )
            
            discharge_disposition = np.random.choice(
                ['home', 'transfer', 'hospice', 'expired', 'other'],
                p=[0.7, 0.15, 0.05, 0.05, 0.05]
            )
            
            admissions.append({
                'admission_id': admission_id,
                'patient_id': patient_id,
                'admission_date': admission_date,
                'discharge_date': discharge_date,
                'admission_type': admission_type,
                'discharge_disposition': discharge_disposition,
                'readmission_status': False  # Will be updated later
            })
            
            admission_id += 1
    
    admissions_df = pd.DataFrame(admissions)
    
    # Calculate readmission status (simplified for demo)
    admissions_df = admissions_df.sort_values(['patient_id', 'admission_date'])
    admissions_df['next_admission_date'] = admissions_df.groupby('patient_id')['admission_date'].shift(-1)
    admissions_df['days_to_readmission'] = (admissions_df['next_admission_date'] - admissions_df['discharge_date']).dt.days
    admissions_df['readmission_status'] = admissions_df['days_to_readmission'] <= 30
    
    # Clean up
    admissions_df = admissions_df.drop(columns=['next_admission_date', 'days_to_readmission'])
    
    # Save sample data
    patients.to_csv('data/raw/patients.csv', index=False)
    admissions_df.to_csv('data/raw/admissions.csv', index=False)
    
    logger.info(f"Generated sample data: {len(patients)} patients, {len(admissions_df)} admissions")
    
    return patients, admissions_df

def run_etl_pipeline():
    """Run the ETL pipeline to process the hospital data."""
    logger.info("Starting ETL pipeline...")
    
    # Initialize the ETL pipeline
    etl = ETLPipeline()
    
    # Run ETL for each data source
    logger.info("Processing patient data...")
    etl.run_patient_etl('data/raw/patients.csv')
    
    logger.info("Processing admission data...")
    etl.run_admission_etl('data/raw/admissions.csv')
    
    logger.info("Running readmission analysis...")
    etl.run_readmission_analysis()
    
    logger.info("ETL pipeline completed successfully")

def analyze_readmission_rates():
    """Analyze and visualize readmission rates."""
    logger.info("Analyzing readmission rates...")
    
    # Load processed data
    try:
        readmission_data = pd.read_parquet('data/processed/readmission_analysis.parquet')
    except FileNotFoundError:
        logger.error("Processed readmission data not found. Run the ETL pipeline first.")
        return
    
    # Basic statistics
    readmission_rate = readmission_data['was_readmitted'].mean() * 100
    avg_days_to_readmit = readmission_data[readmission_data['was_readmitted']]['days_to_readmission'].mean()
    
    print(f"\n=== Readmission Analysis ===")
    print(f"Readmission rate: {readmission_rate:.2f}%")
    print(f"Average days to readmission: {avg_days_to_readmit:.1f} days")
    print(f"Total patients: {readmission_data['patient_id'].nunique()}")
    print(f"Total admissions: {len(readmission_data)}")
    print(f"Readmissions: {readmission_data['was_readmitted'].sum()}")
    
    # Visualizations
    plt.figure(figsize=(14, 6))
    
    # Readmission by admission type
    plt.subplot(1, 2, 1)
    readmission_by_type = readmission_data.groupby('admission_type')['was_readmitted'].mean().sort_values(ascending=False)
    sns.barplot(x=readmission_by_type.index, y=readmission_by_type.values * 100)
    plt.title('Readmission Rate by Admission Type')
    plt.ylabel('Readmission Rate (%)')
    plt.xticks(rotation=45)
    
    # Readmission by age group
    plt.subplot(1, 2, 2)
    readmission_by_age = readmission_data.groupby('age_group')['was_readmitted'].mean().sort_index()
    sns.barplot(x=readmission_by_age.index, y=readmission_by_age.values * 100)
    plt.title('Readmission Rate by Age Group')
    plt.ylabel('Readmission Rate (%)')
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    
    # Save the figure
    os.makedirs('reports', exist_ok=True)
    plt.savefig('reports/readmission_analysis.png')
    plt.show()
    
    logger.info("Readmission analysis completed")

def main():
    """Main function to run the demonstration."""
    try:
        # Set up the environment
        setup_environment()
        
        # Generate sample data if it doesn't exist
        if not os.path.exists('data/raw/patients.csv') or not os.path.exists('data/raw/admissions.csv'):
            generate_sample_data()
        
        # Run the ETL pipeline
        run_etl_pipeline()
        
        # Analyze readmission rates
        analyze_readmission_rates()
        
        logger.info("Demonstration completed successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
