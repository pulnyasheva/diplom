import psycopg2
import random
import string
import time
import os

def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def random_array():
    return [random.randint(1, 100) for _ in range(random.randint(1, 10))]

def connect_db():
    attempts = 5
    for attempt in range(attempts):
        try:
            conn = psycopg2.connect(
                dbname='postgres',
                user='postgres',
                password='postgres',
                host='postgres',
                port='5432'
            )
            print("Подключение успешно!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Ошибка подключения: {e}. Попытка {attempt + 1} из {attempts}.")
            time.sleep(5)

    print("Не удалось подключиться к базе данных после 5 попыток.")
    return None

def populate_tables(conn):
    cursor = conn.cursor()
    i = int(os.getenv('START_ID'))

    while True:
        if (i % 100 == 0):
            print("Operation ", i)

        cursor.execute("""
            INSERT INTO example1 (_id, varchar_field1, varchar_field2, int_field, double_field, text_field, bit_field, bool_field)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            i + 1,
            random_string(10),
            random_string(20),
            random.randint(1, 100),
            random.uniform(1.0, 100.0),
            random_string(30),
            '1' if random.choice([True, False]) else '0',
            random.choice([True, False])
        ))

        cursor.execute("""
            INSERT INTO example2 (_id, int_array)
            VALUES (%s, %s);
        """, (
            i + 1,
            random_array()
        ))

        if (i + 1) % 90 == 0:
            cursor.execute("""
                UPDATE example1
                SET varchar_field1 = %s
                WHERE _id = (SELECT _id FROM example1 ORDER BY RANDOM() LIMIT 1);
            """, (random_string(10),))

        if (i + 1) % 40 == 0:
            cursor.execute("""
                DELETE FROM example1
                WHERE _id = (SELECT _id FROM example1 ORDER BY RANDOM() LIMIT 1);
            """)

            cursor.execute("""
                DELETE FROM example2
                WHERE _id = (SELECT _id FROM example2 ORDER BY RANDOM() LIMIT 1);
            """)

        conn.commit()
        i += 1
        time.sleep(0.9)

    cursor.close()

if __name__ == '__main__':
    conn = connect_db()
    if conn:
        try:
            populate_tables(conn)
        finally:
            conn.close()
