import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

# Base directory
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
REPORTS_DIR = BASE_DIR / 'reports'

# Create directories if they don't exist
for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Database configuration
DB_CONFIG = {
    'drivername': 'postgresql',
    'username': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'hospital_dss')
}

# Data mining configuration
MODEL_CONFIG = {
    'random_state': 42,
    'test_size': 0.2,
    'n_jobs': -1
}

# Feature engineering parameters
FEATURE_PARAMS = {
    'readmission_window_days': 30,  # Consider readmissions within 30 days
    'min_patient_visits': 3,  # Minimum visits for readmission analysis
    'feature_importance_threshold': 0.01  # Threshold for feature selection
}

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': BASE_DIR / 'hospital_dss.log',
            'formatter': 'standard',
            'level': 'DEBUG',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': True
        },
    }
}
