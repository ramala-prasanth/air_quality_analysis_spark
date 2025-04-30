import socket
import time
import csv

# Configuration
HOST = "localhost"
PORT = 9999
DELAY_SECONDS = 2  # Delay between each line sent (simulate real-time)

# Path to your CSV file (should have no header)
CSV_FILE = "task4_input.csv"  # <-- change path as needed

def stream_csv_data():
    # Set up socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen(1)
        print(f"📡 Server listening on {HOST}:{PORT}...")

        conn, addr = s.accept()
        with conn:
            print(f"🔗 Connection from {addr}")

            with open(CSV_FILE, "r") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue
                    line = ",".join(row) + "\n"
                    conn.sendall(line.encode("utf-8"))
                    print(f"✅ Sent: {line.strip()}")
                    time.sleep(DELAY_SECONDS)

if __name__ == "__main__":
    stream_csv_data()
