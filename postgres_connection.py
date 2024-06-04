import psycopg2

# Connect to your PostgreSQL database
def connect_to_postgres(dbname, user, password, host, port):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("Connected to the PostgreSQL database successfully!")
        return conn
    except psycopg2.Error as e:
        print("Unable to connect to the database")
        print(e)
        return None


def export_table_to_csv(connection, table_name, output_file):
    try:
        cursor = connection.cursor()
        with open(output_file, 'w') as f:
            cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH CSV HEADER", f)
        print(f"Data from table '{table_name}' successfully exported to '{output_file}'")
        
    except psycopg2.Error as e:
        print(f"Error exporting data from table '{table_name}' to CSV file")
        print(e)


# Example usage
if __name__ == "__main__":
    dbname = 'your_db_name'
    user = 'your_username'
    password = 'your_password'
    host = 'your_host'  # 'localhost' if it's your local machine
    port = 'your_port_number'  # Default PostgreSQL port is 5432
    connection = connect_to_postgres(dbname, user, password, host, port)
    if connection:
        # Perform database operations here
        export_table_to_csv(connection, table_name, output_file)
        connection.close()  # Close the connection when done

# give a copy command to save the data in csv files