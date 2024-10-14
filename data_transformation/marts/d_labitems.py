import configparser
from ..data_transfer.utils import Connection, DataTransfer


config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']

dt = DataTransfer(Connection(*(x[1] for x in config.items('database'))))

dt.run_query(
    """
    CREATE VIEW mart.d_labitems AS (
        SELECT *
        FROM raw.d_labitems
        WHERE label IN ('Sodium'
            --, 'Sodium, Ascites'
            --, 'Sodium, Body Fluid'
            , 'Sodium, CSF'
            --, 'Sodium, Joint Fluid'
            --, 'Sodium, Pleural'
            --, 'Sodium, Stool'
            , 'Sodium, Urine'
            , 'Sodium, Whole Blood'
            
            , 'Potassium'
            --, 'Potassium, Ascites'
            --, 'Potassium, Body Fluid'
            , 'Potassium, CSF'
            --, 'Potassium, Joint Fluid'
            --, 'Potassium, Pleural'
            --, 'Potassium, Stool'
            , 'Potassium, Urine'
            , 'Potassium, Whole Blood'
    
            --, '24 hr Calcium'
            --, 'Calcium, Body Fluid'
            --, 'Calcium Carbonate Crystals'
            --, 'Calcium Oxalate Crystals'
            --, 'Calcium Phosphate Crystals'
            , 'Calcium, Total'
            --, 'Calcium, Urine'
            , 'Free Calcium'
            , '% Ionized Calcium'
            , 'Total Calcium'
            
            , 'Magnesium'
            , 'Magnesium, Body Fluid'
            --, 'Magnesium, Urine'
    
            , 'Glucose'
            , 'Glucose, Ascites'
            , 'Glucose, Body Fluid'
            , 'Glucose, CSF'
            --, 'Glucose, Joint Fluid'
            --, 'Glucose, Pleural'
            --, 'Glucose, Stool'
            --, 'Glucose, Urine'
            --, 'Glucose, Whole Blood'
    
            , 'Lactate'
            --, 'Lactate Dehydrogenase, Ascites'
            , 'Lactate Dehydrogenase, CSF'
            , 'Lactate Dehydrogenase (LD)'
            --, 'Lactate Dehydrogenase, Pleural'
            --, 'Lactate Dehydrogenase, Stool'
    
            , 'pH'
            , 'pH, Urine'
            
            , 'pO2'
            , 'pO2, Body Fluid'
            , 'pCO2, Body Fluid'
            
            , 'Bicarbonate'
            --, 'Bicarbonate, Ascites'
            , 'Bicarbonate, CSF'
            --, 'Bicarbonate, Other Fluid'
            --, 'Bicarbonate, Pleural'
            --, 'Bicarbonate, Stool'
            , 'Bicarbonate, Urine'
            --, 'Bicarbonate,Joint Fluid'
            , 'Calculated Bicarbonate'
            , 'Calculated Bicarbonate, Whole Blood'
            
            , 'INR(PT)'
            
            , 'PT'
            --, 'PT Control'
            --, 'PT Mean'
            , 'PTT'
            --, 'PTT Control'
            --, 'PTT-LA'
            --, 'PTT mea'
    
            , 'C-Reactive Protein'
            
            , 'White Blood Cells'
            
            , 'Ammonia'
            
            , '(Albumin)'
            , '<Albumin>'
            , 'Albumin'
            --, 'Albumin, Ascites'
            , 'Albumin, Blood'
            --, 'Albumin, Body Fluid'
            , 'Albumin/Creatinine, Urine'
            , 'Albumin, CSF'
            --, 'Albumin, Joint Fluid'
            --, 'Albumin, Pleural'
            --, 'Albumin, Stool'
            --, 'Albumin, Urine'
            --, 'Surfactant/Albumin'
            
            --, '24 hr Creatinine'
            --, 'Albumin/Creatinine, Urine'
            --, 'Amylase/Creatinine Clearance'
            --, 'Amylase/Creatinine Ratio, Urine'
            , 'Creatinine'
            --, 'Creatinine, Ascites'
            , 'Creatinine, Blood'
            --, 'Creatinine, Body Fluid'
            , 'Creatinine Clearance'
            , 'Creatinine, CSF'
            --, 'Creatinine, Joint Fluid'
            --, 'Creatinine, Pleural'
            , 'Creatinine, Serum'
            --, 'Creatinine, Stool'
            --, 'Creatinine, Urine'
            --, 'Creatinine, Whole Blood'
            --, 'Protein/Creatinine Ratio'
            --, 'Urine  Creatinine'
            --, 'Urine Creatinine'
    
            , 'Urea Nitrogen'
            --, 'Urea Nitrogen, Ascites'
            --, 'Urea Nitrogen, Body Fluid'
            , 'Urea Nitrogen, CSF'
            --, 'Urea Nitrogen, Joint Fluid'
            --, 'Urea Nitrogen, Pleural'
            --, 'Urea Nitrogen, Stool'
            --, 'Urea Nitrogen, Urine'
            
            , 'Alanine'
            , 'Alanine Aminotransferase (ALT)'
            , 'Asparate Aminotransferase (AST)'
            
            , 'Bilirubin'
            --, 'Bilirubin Crystals'
            , 'Bilirubin, Direct'
            , 'Bilirubin, Indirect'
            --, 'Bilirubin, Neonatal'
            --, 'Bilirubin, Neonatal, Direct'
            --, 'Bilirubin, Neonatal, Indirect'
            , 'Bilirubin, Total'
            --, 'Bilirubin, Total, Ascites'
            --, 'Bilirubin, Total, Body Fluid'
            , 'Bilirubin, Total, CSF'
            --, 'Bilirubin, Total, Joint Fluid'
            --, 'Bilirubin, Total, Pleural'
            --, 'Bilirubin, Total, Stool'
            
            --, 'Hemoglobin F'
            --, 'Hemoglobin Other'
            , '% Hemoglobin A1c'
            --, 'Hemoglobin  A2'
            , 'Hemoglobin'
            --, 'Hemoglobin  C'
            --, 'Hemoglobin  A1'
            --, 'Hemoglobin A2'
            --, 'P50 of Hemoglobin'
            , 'Plasma Hemoglobin'
            --, 'Hemoglobin  S'
            --, 'Hemoglobin, Calculated'
            --, 'Fetal Hemoglobin'
            --, 'Glycated Hemoglobin'
            --, 'Hemoglobin H Inclusion'
            --, 'Absolute Hemoglobin'
            --, 'Reticulocyte, Cellular Hemoglobin'
            --, 'Hemoglobin  A'
            --, 'Hemoglobin C'
            --, 'Hemoglobin  F'
    
            , 'Hematocrit'
            --, 'Hematocrit, Ascites'
            --, 'Hematocrit, Calculated'
            , 'Hematocrit, CSF'
            --, 'Hematocrit, Joint Fluid'
            --, 'Hematocrit, Other Fluid'
            --, 'Hematocrit, Pleural'

    ))
    """
)
