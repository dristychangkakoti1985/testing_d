import pandas as pd
import requests
import zipfile
import io
205
# URL to the MSHA Accident/Injury dataset ZIP file
zip_url = 'https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA/Accidents.zip'

# Download the ZIP file
response = requests.get(zip_url)
if response.status_code == 200:
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # List all files in the ZIP
        print("Files in ZIP:", z.namelist())
        # Assuming the CSV file is named 'Accidents.txt'
        with z.open('Accidents.txt') as f:
            # Load the dataset into a pandas DataFrame
            df = pd.read_csv(f, sep='|', encoding='latin1')
else:
    print(f"Failed to download file. Status code: {response.status_code}")
