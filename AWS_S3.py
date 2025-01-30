import os
import csv
from datetime import datetime

# The directory you want to iterate through
folder_path = '/Users/sachinkumar/Downloads/images'

# The CSV file where you want to save the file details
csv_file_path = 'imagesDetails.csv'

# Get a list of files in the directory
file_names = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

# Open the CSV file for writing
with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    # Write the header row
    writer.writerow(['ProductID', 'Link'])
    
    # Iterate over each file
    for file_name in file_names:
        file_path = os.path.join(folder_path, file_name)
        
        # Get file size
        size = os.path.getsize(file_path)
        
        # Get last modification time and format it
        mod_time = os.path.getmtime(file_path)
        last_modified_date = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S')
        
        # Write file details to
        writer.writerow([file_name[:-4], f'https://upload.meeshosupplyassets.com/temp/{file_name}'])
# Example usage

