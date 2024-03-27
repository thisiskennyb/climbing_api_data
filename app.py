from flask import Flask
import psycopg2
import csv
import subprocess
import os

DB_NAME = 'boulder_db'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_HOST = '127.0.0.1'
DB_PORT = '5454'  # Default PostgreSQL port

app = Flask(__name__)

def create_table():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    create_table_query = """
    CREATE TABLE IF NOT EXISTS boulders (
        id SERIAL PRIMARY KEY,
        crag VARCHAR(255),
        name VARCHAR(255),
        grade VARCHAR(255),
        ascents INTEGER
    )
    """

    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    
def pull_from_github():
    # Define the Git commands
    pull_command = 'git pull origin master'
    result = subprocess.run(pull_command, shell=True)
    if result.returncode != 0:
        print(f"Error executing command: {pull_command}")
    else:
        print(f"Command executed successfully: {pull_command}")

def seed_database(csv_file_path):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()

    folder_path = 'climbing_areas_output/'

    # Read the list of processed CSV files from the text file
    processed_files = set()
    if os.path.exists('processed_files.txt'):
        with open('processed_files.txt', 'r') as f:
            processed_files = set(f.read().splitlines())

    for filename in os.listdir(folder_path):
        if filename not in processed_files:
            print(f"Processing {filename}...")
            with open(os.path.join(folder_path, filename), 'r') as file:
                data = csv.reader(file)
                next(data)  # Skip header if needed

                for row in data:
                    crag, name, grade, ascents = row
                    cursor.execute("INSERT INTO boulders (crag, name, grade, ascents) VALUES (%s, %s, %s, %s)", (crag, name, grade, ascents))
                
                conn.commit()
                print(f'Data from {filename} successfully seeded into the database')

            # Record the processed CSV file in the text file
            processed_files.add(filename)

    cursor.close()
    conn.close()

    # Write the updated list of processed CSV files back to the text file
    with open('processed_files.txt', 'w') as f:
        f.write('\n'.join(processed_files))


if __name__ == '__main__':
    create_table()  # Ensure the table exists before seeding data
    csv_file_path = 'climbing_areas_output/cumberland.csv'  # Update with the path to your CSV file
    seed_database(csv_file_path)
    app.run(debug=True)
