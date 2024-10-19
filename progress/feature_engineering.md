# Miscellaneous
- [X] Admission month
- [X] Admission time of day

# Labevents 
### 1. Electrolytes & Metabolic Panel
- [X] Sodium (Na)
- [X] Potassium (K)
- [X] Calcium (Ca)
- [X] Magnesium (Mg)
- [X] Blood Glucose
- [X] Lactate

### 2. Arterial Blood Gases (ABG)
- [X] pH
- [X] PaO2 (Partial pressure of oxygen)
- [X] PaCO2 (Partial pressure of carbon dioxide)
- [X] HCO3 (Bicarbonate)

### 3. Coagulation Profile
- [X] INR (International Normalized Ratio)
- [X] PT (Prothrombin Time)
- [X] PTT (Partial Thromboplastin Time)

### 4. Inflammatory Markers
- [X] C-Reactive Protein (CRP)
- [X] White Blood Cell (WBC) Count

### 5. Neurological-specific Biomarkers
- [X] Ammonia (in cases of hepatic encephalopathy)
- [X] Albumin (can indicate malnutrition or infection)
- [X] Cerebrospinal fluid (CSF) markers (if available)

### 6. Renal Function
- [X] Creatinine
- [X] Blood Urea Nitrogen (BUN)

### 7. Liver Function Tests
- [X] ALT/AST (Alanine/Aspartate Transaminase)
- [X] Bilirubin

### 8. Hemoglobin and Hematocrit
- [X] Hemoglobin
- [X] Hematocrit

### 9. Miscellaneous
- [X] Min value (for each feature from above)
- [X] Max value
- [X] Avg value
- [X] Upper bound from range of normal results
- [X] Lower bound from range of normal results
- [X] If at least one of the tests was abnormal
- [X] Ratio No. of Abnormal tests / all tests
- [X] First abnormal charttime
- [X] First test charttime
- [X] Last test charttime
- [X] Hours between the first abnormal test and discharge

# Chartevents 
### 1. Vital Signs
- [X] Heart Rate (HR)
- [X] Respiratory Rate (RR)
- [X] Blood Pressure (BP): Systolic and Diastolic
- [X] Oxygen Saturation (SpO2)
- [X] Temperature
### 2. Neurological Monitoring
- [X] Glasgow Coma Scale (GCS): Scores like eye response, verbal response, and motor response provide key information about neurological function.
- [X] Pupillary Response: Measures of pupil size and reactivity are critical in assessing neurological deterioration.
### 3. Lab Results Related to Neurological Conditions
- [X] Blood Gas Analysis: Arterial pH, pCO2, pO2 can reflect respiratory function and metabolic state, which can affect brain function.
- [X] Serum Sodium: Dysnatremias (particularly hyponatremia) are common in neurological patients.
- [ ] Lactate Levels: High lactate levels may indicate tissue hypoxia, a concerning sign in critically ill patients.
### 4. Fluid and Electrolytes
- [X] Serum Sodium/Potassium/Chloride: Imbalances in electrolytes can impact neurological function and are common in critically ill patients.
- [X] Blood Urea Nitrogen (BUN) and Creatinine: Renal function indicators, which may worsen during ICU stay and impact recovery.
### 5. Respiratory and Cardiovascular Support
- [X] Ventilator Settings/Parameters: Mechanical ventilation settings (tidal volume, PEEP, FiO2) and blood gas results can show respiratory compromise, which may predict longer ICU stays.
- [ ] Inotrope/Vasopressor Use: Indicators of cardiovascular instability, which can affect neurological outcomes and LOS.
### 6. Sedation and Pain Management
- [X] Sedation Scales: Monitoring for sedation depth (e.g., Richmond Agitation-Sedation Scale) as this impacts neurological assessments.
- [ ] Opioid and Sedative Administration: These drugs are often needed for patients in the ICU but can cloud neurological examination.
### 7. Complication-Related Tests
- [X] Coagulation Profile: Tests like prothrombin time (PT), INR, and partial thromboplastin time (PTT) can signal coagulopathies, which are complications in critically ill neurological patients.
- [ ] Liver Function Tests (AST/ALT, Bilirubin): Liver dysfunction may occur during critical illness and affect brain function.
