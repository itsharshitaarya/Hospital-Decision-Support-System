#!/usr/bin/env python3
"""
Hospital Decision Support System - Web Interface

A Flask web application to upload, process, and visualize hospital data.
"""

import os
import json
import pandas as pd
from pathlib import Path
from flask import (
    Flask, render_template, jsonify, 
    request, redirect, url_for, flash
)
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.secret_key = 'your-secret-key-here'  # For flash messages

# Configure upload folder
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Ensure upload folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Configuration
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
PROCESSED_DIR = DATA_DIR / 'processed'
REPORTS_DIR = BASE_DIR / 'reports'

# Ensure required directories exist
for directory in [DATA_DIR, PROCESSED_DIR, REPORTS_DIR]:
    directory.mkdir(exist_ok=True, parents=True)

class PatientData:
    def __init__(self):
        self.recent_admissions = [
            {'patient_id': 42, 'admission_date': '2023-05-15', 'discharge_date': '2023-05-18', 'admission_type': 'emergency'},
            {'patient_id': 87, 'admission_date': '2023-05-14', 'discharge_date': '2023-05-16', 'admission_type': 'elective'}
        ]
        self.readmission_rate = 12.5
        self.avg_length_of_stay = 4.2
        self.readmission_by_age = {'<30': 8.2, '30-50': 10.5, '50-70': 15.3, '70+': 18.7}
        self.readmission_by_gender = {'M': 13.2, 'F': 11.8}
        self.readmission_by_admission_type = {'emergency': 14.3, 'urgent': 11.2, 'elective': 8.7}
    
    def add_patient(self, patient_id, admission_date, admission_type):
        # Add new patient to the beginning of the list
        self.recent_admissions.insert(0, {
            'patient_id': patient_id,
            'admission_date': admission_date,
            'discharge_date': None,  # No discharge date yet
            'admission_type': admission_type
        })
        # Keep only the 10 most recent admissions
        self.recent_admissions = self.recent_admissions[:10]
        return self.recent_admissions

# Initialize patient data
patient_data = PatientData()

def load_sample_data():
    """Load current patient data."""
    return {
        'readmission_rate': patient_data.readmission_rate,
        'avg_length_of_stay': patient_data.avg_length_of_stay,
        'readmission_by_age': patient_data.readmission_by_age,
        'readmission_by_gender': patient_data.readmission_by_gender,
        'readmission_by_admission_type': patient_data.readmission_by_admission_type,
        'recent_admissions': patient_data.recent_admissions
    }

# Routes
@app.route('/', methods=['GET', 'POST'])
def index():
    """Render the main dashboard and handle file uploads and patient additions."""
    if request.method == 'POST':
        form_type = request.form.get('form_type')
        
        # Handle file upload
        if form_type == 'upload':
            # Check if the post request has the file part
            if 'file' not in request.files:
                flash('No file part', 'error')
                return redirect(request.url)
            
            file = request.files['file']
            
            # If user does not select file, browser submits an empty part
            if file.filename == '':
                flash('No selected file', 'error')
                return redirect(request.url)
            
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(filepath)
                
                try:
                    # Process the uploaded file
                    if filename.endswith('.csv'):
                        df = pd.read_csv(filepath)
                    else:  # Excel file
                        df = pd.read_excel(filepath)
                    
                    # Save a sample of the data to display
                    sample_data = df.head(10).to_dict('records')
                    columns = list(df.columns)
                    
                    flash('File successfully uploaded and processed!', 'success')
                    return render_template('index.html', 
                                       uploaded_data=sample_data,
                                       columns=columns,
                                       filename=filename,
                                       **load_sample_data())
                    
                except Exception as e:
                    flash(f'Error processing file: {str(e)}', 'error')
                    return redirect(request.url)
        
        # Handle manual patient addition
        elif form_type == 'add_patient':
            try:
                patient_id = request.form.get('patient_id')
                admission_date = request.form.get('admission_date')
                admission_type = request.form.get('admission_type')
                
                # Add the new patient
                patient_data.add_patient(patient_id, admission_date, admission_type)
                
                # Show success message
                flash(f'Successfully added patient {patient_id} with {admission_type} admission on {admission_date}', 'success')
                
            except Exception as e:
                flash(f'Error adding patient: {str(e)}', 'error')
    
    # For GET requests or after form submission
    data = load_sample_data()
    return render_template('index.html', **data)

@app.route('/api/readmission_rates')
def get_readmission_rates():
    """API endpoint for readmission rate data."""
    data = load_sample_data()
    return jsonify({
        'readmission_by_age': data['readmission_by_age'],
        'readmission_by_gender': data['readmission_by_gender'],
        'readmission_by_admission_type': data['readmission_by_admission_type']
    })

@app.route('/api/recent_admissions')
def get_recent_admissions():
    """API endpoint for recent admissions data."""
    return jsonify({'recent_admissions': patient_data.recent_admissions})

# Create templates directory if it doesn't exist
templates_dir = BASE_DIR / 'templates'
templates_dir.mkdir(exist_ok=True)

# Create index.html template
index_html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hospital Decision Support System</title>
    <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        .stat-card {
            @apply bg-white p-6 rounded-lg shadow-md hover:shadow-lg transition-shadow;
        }
        .stat-value {
            @apply text-3xl font-bold text-blue-600;
        }
        .stat-label {
            @apply text-gray-600 mt-2;
        }
    </style>
</head>
<body class="bg-gray-100">
    <div class="min-h-screen">
        <!-- Header -->
        <header class="bg-blue-600 text-white p-6 shadow-md">
            <div class="container mx-auto">
                <h1 class="text-3xl font-bold">Hospital Decision Support System</h1>
                <p class="mt-2">Data-driven insights for better patient care</p>
            </div>
        </header>

        <!-- Main Content -->
        <main class="container mx-auto p-6">
            <!-- Stats Overview -->
            <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
                <div class="stat-card">
                    <div class="stat-value">{{ '%.1f'|format(readmission_rate) }}%</div>
                    <div class="stat-label">Readmission Rate</div>
                    <p class="text-sm text-gray-500 mt-2">Percentage of patients readmitted within 30 days</p>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{ '%.1f'|format(avg_length_of_stay) }} days</div>
                    <div class="stat-label">Average Length of Stay</div>
                    <p class="text-sm text-gray-500 mt-2">Average duration of hospital stays</p>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{ recent_admissions|length }}</div>
                    <div class="stat-label">Recent Admissions</div>
                    <p class="text-sm text-gray-500 mt-2">Most recent patient admissions</p>
                </div>
            </div>

            <!-- Charts -->
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                <div class="bg-white p-6 rounded-lg shadow-md">
                    <h2 class="text-xl font-semibold mb-4">Readmission Rate by Age Group</h2>
                    <canvas id="ageChart"></canvas>
                </div>
                <div class="bg-white p-6 rounded-lg shadow-md">
                    <h2 class="text-xl font-semibold mb-4">Readmission Rate by Gender</h2>
                    <canvas id="genderChart"></canvas>
                </div>
                <div class="bg-white p-6 rounded-lg shadow-md">
                    <h2 class="text-xl font-semibold mb-4">Readmission Rate by Admission Type</h2>
                    <canvas id="admissionTypeChart"></canvas>
                </div>
                <div class="bg-white p-6 rounded-lg shadow-md">
                    <h2 class="text-xl font-semibold mb-4">Recent Admissions</h2>
                    <div class="overflow-x-auto">
                        <table class="min-w-full divide-y divide-gray-200">
                            <thead class="bg-gray-50">
                                <tr>
                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Patient ID</th>
                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Admission Date</th>
                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                                </tr>
                            </thead>
                            <tbody id="recentAdmissions" class="bg-white divide-y divide-gray-200">
                                {% for admission in recent_admissions %}
                                <tr>
                                    <td class="px-6 py-4 whitespace-nowrap">{{ admission.patient_id }}</td>
                                    <td class="px-6 py-4 whitespace-nowrap">{{ admission.admission_date }}</td>
                                    <td class="px-6 py-4 whitespace-nowrap">
                                        <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full 
                                            {% if admission.admission_type == 'emergency' %}bg-red-100 text-red-800
                                            {% elif admission.admission_type == 'urgent' %}bg-yellow-100 text-yellow-800
                                            {% else %}bg-green-100 text-green-800{% endif %}">
                                            {{ admission.admission_type|title }}
                                        </span>
                                    </td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </main>

        <!-- Footer -->
        <footer class="bg-gray-800 text-white p-6 mt-12">
            <div class="container mx-auto text-center">
                <p>© 2023 Hospital Decision Support System. All rights reserved.</p>
            </div>
        </footer>
    </div>

    <script>
        // Fetch data from API endpoints
        Promise.all([
            fetch('/api/readmission_rates').then(res => res.json()),
            fetch('/api/recent_admissions').then(res => res.json())
        ]).then(([ratesData, admissionsData]) => {
            // Update recent admissions
            const recentAdmissions = document.getElementById('recentAdmissions');
            recentAdmissions.innerHTML = admissionsData.recent_admissions.map(admission => `
                <tr>
                    <td class="px-6 py-4 whitespace-nowrap">${admission.patient_id}</td>
                    <td class="px-6 py-4 whitespace-nowrap">${admission.admission_date}</td>
                    <td class="px-6 py-4 whitespace-nowrap">
                        <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full 
                            ${admission.admission_type === 'emergency' ? 'bg-red-100 text-red-800' : 
                              admission.admission_type === 'urgent' ? 'bg-yellow-100 text-yellow-800' : 
                              'bg-green-100 text-green-800'}">
                            ${admission.admission_type.charAt(0).toUpperCase() + admission.admission_type.slice(1)}
                        </span>
                    </td>
                </tr>
            `).join('');

            // Create charts
            createBarChart('ageChart', 
                Object.keys(ratesData.readmission_by_age), 
                Object.values(ratesData.readmission_by_age),
                'Readmission Rate (%)',
                'rgba(54, 162, 235, 0.6)'
            );

            createBarChart('genderChart', 
                Object.keys(ratesData.readmission_by_gender), 
                Object.values(ratesData.readmission_by_gender),
                'Readmission Rate (%)',
                'rgba(255, 99, 132, 0.6)'
            );

            createBarChart('admissionTypeChart', 
                Object.keys(ratesData.readmission_by_admission_type), 
                Object.values(ratesData.readmission_by_admission_type),
                'Readmission Rate (%)',
                'rgba(75, 192, 192, 0.6)'
            );
        });

        function createBarChart(canvasId, labels, data, label, backgroundColor) {
            const ctx = document.getElementById(canvasId).getContext('2d');
            new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: [{
                        label: label,
                        data: data,
                        backgroundColor: backgroundColor,
                        borderColor: backgroundColor.replace('0.6', '1'),
                        borderWidth: 1
                    }]
                },
                options: {
                    responsive: true,
                    scales: {
                        y: {
                            beginAtZero: true,
                            title: {
                                display: true,
                                text: 'Readmission Rate (%)'
                            }
                        },
                        x: {
                            title: {
                                display: true,
                                text: canvasId === 'ageChart' ? 'Age Group' : 
                                      canvasId === 'genderChart' ? 'Gender' : 'Admission Type'
                            }
                        }
                    }
                }
            });
        }
    </script>
</body>
</html>
"""

# Write the template file
with open(templates_dir / 'index.html', 'w') as f:
    f.write(index_html)

if __name__ == '__main__':
    # Create necessary directories
    (BASE_DIR / 'static').mkdir(exist_ok=True)
    
    # Run the Flask app
    port = 5050
    print("Starting Hospital Decision Support System web interface...")
    print(f"Open your browser and navigate to: http://localhost:{port}")
    app.run(debug=True, host='0.0.0.0', port=port)
