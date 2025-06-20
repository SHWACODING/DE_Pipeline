import subprocess
import time

def wait_for_postgres (host, max_retires=5, delay_seconds=5) :
    retries = 0
    while retries < max_retires :
        try :
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True
            )
            if "accepting connection" in result.stdout:
                print("Successfully Connected To Postgres Server")
                return True
        except subprocess.CalledProcessError as e :
            print(f"Error Connecting To Postgres Server : {e}")
            retries += 1
            print(f'Retrying In {delay_seconds} Seconds... & Waiting for PostgreSQL at {host} (Attempt {retries+1}/{max_retires})')
            time.sleep(delay_seconds)
    print(f'Failed to connect to PostgreSQL at {host} after {max_retires} attempts')
    return False

if not wait_for_postgres(host="source_postgres"):
    exit(1)

print("Starting ELT Script.....")

source_config = {
    "dbname": "source_db",
    "user": "postgres",
    "password": "secret",
    "host": "source_postgres",
}

destination_config = {
    "dbname": "destination_db",
    "user": "postgres",
    "password": "secret",
    "host": "destination_postgres",
}

dump_command = [
    "pg_dump",
    "-h", source_config["host"],
    "-U", source_config["user"],
    "-d", source_config["dbname"],
    "-f", "data_dump.sql",
    "-w"
]

subprocess_env = dict(PGPASSWORD=source_config['password'])

subprocess.run(dump_command, env=subprocess_env, check=True)

load_command = [
    "psql",
    "-h", destination_config["host"],
    "-U", destination_config["user"],
    "-d", destination_config["dbname"],
    "-a", "-f", "data_dump.sql",
]

subprocess_env = dict(PGPASSWORD=destination_config['password'])

subprocess.run(load_command, env=subprocess_env, check=True)

print("Ending ELT Script..")

