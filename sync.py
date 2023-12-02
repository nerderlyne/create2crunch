import os
import re
import psycopg2
import psycopg2.pool
from psycopg2 import IntegrityError

import logging
from itertools import islice
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

conn_url = os.getenv('DB_URL')
conn_pool = psycopg2.pool.SimpleConnectionPool(1, 10, conn_url)
caller = os.getenv('CALLER')

def count_zeroes(address):
    # Remove the '0x' prefix for correct processing
    address = address[2:]

    # Count leading zero bytes
    # Each byte is represented by two hex characters
    leading_zeroes = len(re.match('^(00)*', address).group()) // 2

    # Count total zero bytes
    total_zeroes = address.count('00')

    return total_zeroes, leading_zeroes

def get_last_line_number():
    conn = conn_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(line_number) FROM crunch WHERE caller_address=%s", (caller,))
        result = cur.fetchone()
        return result[0] if result[0] is not None else 0
    except Exception as e:
        logging.error("Database error in get_last_line_number: %s", e)
        return 0
    finally:
        conn_pool.putconn(conn)

def sync_batch_to_db(batch):
    if not batch:
        return

    conn = conn_pool.getconn()
    cur = conn.cursor()

    try:
        for record in batch:
            try:
                cur.execute("INSERT INTO crunch (salt, address, reward, totalZeroes, leadingZeroes, line_number) VALUES (%s, %s, %s, %s, %s, %s)",
                            record)
            except IntegrityError:
                conn.rollback()
                continue
        conn.commit()
        logging.info("Successfully synced a batch of %s records", len(batch))
    except Exception as e:
        logging.error("Database error in sync_batch_to_db: %s", e)
    finally:
        conn_pool.putconn(conn)

last_line_number = get_last_line_number()
logging.info("Starting from line number %s", last_line_number + 1)

batch = []
batch_size = 1000  # You can adjust this based on your requirements

with open('efficient_addresses.txt', 'r') as file:
    current_line = last_line_number + 1
    while True:
        lines = list(islice(file, batch_size))
        if not lines:
            break

        for line in lines:
            current_line += 1
            if current_line <= last_line_number:
                continue

            parts = line.strip().split(' => ')
            if len(parts) == 3:
                salt, address, reward = parts
                total_zeroes, leading_zeroes = count_zeroes(address)
                batch.append((salt, address, int(reward), total_zeroes, leading_zeroes, current_line))

        sync_batch_to_db(batch)
        batch = []  # Reset batch after syncing

# Sync any remaining records that didn't fill up the last batch
sync_batch_to_db(batch)
