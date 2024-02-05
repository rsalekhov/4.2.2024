import psycopg2
from psycopg2 import sql


def create_db(conn):
    with conn.cursor() as cursor:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR(255) UNIQUE
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS phones (
                id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES clients(id),
                phone_number VARCHAR(20)
            )
        ''')

    conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cursor:
        cursor.execute('''
            INSERT INTO clients (first_name, last_name, email)
            VALUES (%s, %s, %s) RETURNING id
        ''', (first_name, last_name, email))

        client_id = cursor.fetchone()[0]

        if phones:
            for phone in phones:
                cursor.execute('''
                    INSERT INTO phones (client_id, phone_number)
                    VALUES (%s, %s)
                ''', (client_id, phone))

    conn.commit()


def add_phone(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute('''
            INSERT INTO phones (client_id, phone_number)
            VALUES (%s, %s)
        ''', (client_id, phone))

    conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cursor:
        update_fields = []

        if first_name:
            update_fields.append(sql.Identifier('first_name') + sql.SQL(' = %s'))
        if last_name:
            update_fields.append(sql.Identifier('last_name') + sql.SQL(' = %s'))
        if email:
            update_fields.append(sql.Identifier('email') + sql.SQL(' = %s'))

        if update_fields:
            cursor.execute(sql.SQL('''
                UPDATE clients SET {} WHERE id = %s
            ''').format(sql.SQL(', ').join(update_fields)), (first_name, last_name, email, client_id))

        if phones:
            cursor.execute('DELETE FROM phones WHERE client_id = %s', (client_id,))
            for phone in phones:
                cursor.execute('''
                    INSERT INTO phones (client_id, phone_number)
                    VALUES (%s, %s)
                ''', (client_id, phone))

    conn.commit()


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute('DELETE FROM phones WHERE client_id = %s AND phone_number = %s', (client_id, phone))
    conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cursor:
        cursor.execute('DELETE FROM clients WHERE id = %s', (client_id,))
    conn.commit()


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cursor:
        query = '''
            SELECT clients.id, first_name, last_name, email, ARRAY_AGG(phone_number)
            FROM clients
            LEFT JOIN phones ON clients.id = phones.client_id
            WHERE (%s IS NULL OR first_name ILIKE %s)
                AND (%s IS NULL OR last_name ILIKE %s)
                AND (%s IS NULL OR email ILIKE %s)
                AND (%s IS NULL OR phone_number LIKE %s)
            GROUP BY clients.id, first_name, last_name, email
        '''

        cursor.execute(query, (first_name, f'%{first_name}%',
                               last_name, f'%{last_name}%',
                               email, f'%{email}%',
                               phone, f'%{phone}%'))

        results = cursor.fetchall()

    return results


# Пример использования
with psycopg2.connect(database="clients_db", user="postgres", password="postgres") as conn:
    create_db(conn)

    add_client(conn, "John", "Doe", "john.doe@example.com", phones=["+123456789"])
    add_phone(conn, 1, "+987654321")

    print("Before Update:")
    print(find_client(conn, first_name="John"))

    change_client(conn, 1, last_name="Smith", phones=["+111222333", "+444555666"])

    print("After Update:")
    print(find_client(conn, first_name="John"))

    delete_phone(conn, 1, "+987654321")

    print("After Delete Phone:")
    print(find_client(conn, first_name="John"))

    delete_client(conn, 1)

    print("After Delete Client:")
    print(find_client(conn, first_name="John"))

conn.close()
