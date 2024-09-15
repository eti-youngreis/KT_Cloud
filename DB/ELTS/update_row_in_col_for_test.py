import csv
from datetime import datetime

# Function to update a row in the CSV file
def update_row(csv_file, id_to_update, new_data):
    rows = []
    updated = False

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        fieldnames = csv_reader.fieldnames

        for row in csv_reader:
            if row['AlbumId'] == id_to_update:
                row.update(new_data)
                updated = True
            rows.append(row)

    if not updated:
        print(f"Row with ID {id_to_update} not found.")
        return

    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        csv_writer = csv.DictWriter(file, fieldnames=fieldnames)
        csv_writer.writeheader()
        csv_writer.writerows(rows)

    print(f"Row with ID {id_to_update} updated successfully.")

# Specify the CSV file, ID to update, and new data
csv_file = 'DB\csv_files\Album.csv'
id_to_update = '1'
new_data = {'Title': 'Updated_title', 'ArtistId': 2, 'updated_at': datetime.now()}
new_data_2 = {'Title': 'For Those About To Rock We Salute You',\
 'ArtistId': 1, 'updated_at': datetime.now()}

# Call the function to update the row
update_row(csv_file, id_to_update, new_data)
