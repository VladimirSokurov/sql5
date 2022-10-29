import psycopg2

phones_list_1 = [89506088475, 89087684963]
phones_list_2 = [123433333678901, 1987653333343212]
phones_list_3 = [12345678901, 19876543212]


def get_client_id(cursor, name: str) -> int:
    cursor.execute("""
    SELECT id FROM Clients WHERE name=%s;
    """, (name,))
    return cur.fetchone()[0]


# так не работает:
# def get_phone_id(cursor, number: int) -> int:
#     cursor.execute("""
#     SELECT id FROM Numbers WHERE number=%d;
#     """, (number,))
#     return cur.fetchone()[0]

# так работает:
def get_phone_id(cursor, number: int) -> int:
    cursor.execute(f"""
    SELECT id FROM Numbers WHERE number={number};
    """)
    return cur.fetchone()[0]


def create_db(conn):
    cur.execute("""
    DROP TABLE ClientsNumbers;
    DROP TABLE Clients;
    DROP TABLE Numbers;        
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Clients(
            id SERIAL PRIMARY KEY,
            name VARCHAR(40) NOT NULL,
            surname VARCHAR(60) NOT NULL,
            email VARCHAR(100) NOT NULL
            );
            """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Numbers(
            id SERIAL PRIMARY KEY,
            number BIGINT NOT NULL
            );
            """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ClientsNumbers(
            id SERIAL PRIMARY KEY,
            client_id integer not null references Clients(id),
            number_id integer not null references Numbers(id)
            );
            """)
    conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    cur.execute("""
        INSERT INTO Clients(name, surname, email) VALUES
            ('%s', '%s', '%s');
            """ % (first_name, last_name, email))
    client_id = get_client_id(cursor=cur, name=first_name)
    if phones:
        if type(phones) != int:
            for number in phones:
                cur.execute("""
                    INSERT INTO Numbers(number) VALUES
                        (%d);
                        """ % number)

                number_id = get_phone_id(cursor=cur, number=number)

                cur.execute("""
                    INSERT INTO ClientsNumbers(client_id, number_id) VALUES
                        (%d, %d);
                        """ % (client_id, number_id))
        else:
            cur.execute("""
                INSERT INTO Numbers(number) VALUES
                    (%d);
                    """ % phones)
            number_id = get_phone_id(cursor=cur, number=phones)
            cur.execute("""
                INSERT INTO ClientsNumbers(client_id, number_id) VALUES
                    (%d, %d);
                    """ % (client_id, number_id))


def add_phone(conn, client_id, phone):
    if type(phone) != int:
        for number in phone:
            cur.execute("""
                  INSERT INTO Numbers(number) VALUES
                      (%d);
                      """ % number)
            number_id = get_phone_id(cursor=cur, number=number)
            cur.execute("""
                  INSERT INTO ClientsNumbers(client_id, number_id) VALUES
                      (%d, %d);
                      """ % (client_id, number_id))
    else:
        cur.execute("""
                         INSERT INTO Numbers(number) VALUES
                             (%d);
                             """ % phone)
        number_id = get_phone_id(cursor=cur, number=phone)
        cur.execute("""
                         INSERT INTO ClientsNumbers(client_id, number_id) VALUES
                             (%d, %d);
                             """ % (client_id, number_id))


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    if first_name:
        cur.execute("""
        UPDATE Clients SET name=%s WHERE id=%s;
        """, (first_name, client_id))
    if last_name:
        cur.execute("""
        UPDATE Clients SET surname=%s WHERE id=%s;
        """, (last_name, client_id))
    if email:
        cur.execute("""
        UPDATE Clients SET email=%s WHERE id=%s;
        """, (email, client_id))
    if phones:
        cur.execute("""
               SELECT n.id FROM Numbers n
               LEFT JOIN ClientsNumbers cn ON n.id = cn.number_id
               WHERE cn.client_id = %s
               ORDER BY n.id;
               """, (client_id,))
        number_id_ = cur.fetchall()
        cur.execute("""
               DELETE FROM ClientsNumbers WHERE client_id=%s;
               """, (client_id,))
        for id in number_id_:
            cur.execute("""
                   DELETE FROM Numbers WHERE id=%s;
                   """, (id[0],))

        add_phone(conn, client_id, phones)


def delete_phone(conn, client_id, phone):
    cur.execute("""
           SELECT n.id FROM Numbers n
           LEFT JOIN ClientsNumbers cn ON n.id = cn.number_id
           WHERE cn.client_id = %s and n.number = %s
           ORDER BY n.id;
           """, (client_id, phone))
    number_id_ = cur.fetchall()
    cur.execute("""
           DELETE FROM ClientsNumbers WHERE number_id=%s;
           """, (number_id_[0],))

    for id in number_id_:
        cur.execute("""
               DELETE FROM Numbers WHERE id=%s;
               """, (id[0],))


def delete_client(conn, client_id):
    cur.execute("""
           SELECT n.id FROM Numbers n
           LEFT JOIN ClientsNumbers cn ON n.id = cn.number_id
           WHERE cn.client_id = %s
           ORDER BY n.id;
           """, (client_id,))
    number_id_ = cur.fetchall()
    cur.execute("""
           DELETE FROM ClientsNumbers WHERE client_id=%s;
           """, (client_id,))

    for id in number_id_:
        cur.execute("""
               DELETE FROM Numbers WHERE id=%s;
               """, (id[0],))

    cur.execute("""
                  DELETE FROM Clients WHERE id=%s;
                  """, (client_id,))


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    client_list = []
    name_ = ""
    last_name_ = ""
    email_ = ""
    phone_ = ""
    phone_list = []

    if not phone:
        if first_name:
            cur.execute("""
                SELECT * FROM Clients
                WHERE name=%s;
                    """, (first_name,))
            result = cur.fetchall()
            name_ = result[0][1]
            last_name_ = result[0][2]
            email_ = result[0][3]

        elif last_name:
            cur.execute("""
                SELECT * FROM Clients
                WHERE surname=%s;
                    """, (last_name,))
            result = cur.fetchall()
            name_ = result[0][1]
            last_name_ = result[0][2]
            email_ = result[0][3]

        elif email:
            cur.execute("""
                        SELECT * FROM Clients
                        WHERE email=%s;
                            """, (email,))
            result = cur.fetchall()
            name_ = result[0][1]
            last_name_ = result[0][2]
            email_ = result[0][3]

        client_id = get_client_id(cur, name=name_)
        cur.execute("""
            SELECT n.number FROM Numbers n
            LEFT JOIN ClientsNumbers cn ON n.id = cn.number_id
            WHERE client_id=%s
            GROUP BY n.number;
                """, (client_id,))
        result2 = cur.fetchall()
        if len(result2) > 1:
            for i in result2:
                phone_list.append(i[0])
            client_list.append(phone_list)
        else:
            phone_ = result2[0][0]
            client_list.append(phone_)
    else:
        cur.execute("""
                       SELECT * FROM Clients
                       WHERE id=(
                       SELECT client_id from ClientsNumbers
                       WHERE number_id=(SELECT id from Numbers
                       WHERE number=%s));
                           """, (phone,))
        result = cur.fetchall()
        name_ = result[0][1]
        last_name_ = result[0][2]
        email_ = result[0][3]

        cur.execute("""                              
                      SELECT number_id FROM ClientsNumbers
                      WHERE client_id=(
                      SELECT client_id FROM ClientsNumbers
                      WHERE number_id=(SELECT id from Numbers
                      WHERE number=%s));
                          """, (phone,))
        result2 = cur.fetchall()

        numbers_list = []
        for i in result2:
            cur.execute("""
                    SELECT n.number FROM Numbers n
                    LEFT JOIN ClientsNumbers cn ON n.id = cn.number_id
                    WHERE cn.number_id = %s;
                    """, (i[0],))
            result3 = cur.fetchall()
            if len(result2) > 1:
                numbers_list.append(result3[0][0])

            else:
                phone_ = result3[0][0]
                client_list.append(phone_)
        if len(result2) > 1:
            client_list.append(numbers_list)

    client_list.append(name_)
    client_list.append(last_name_)
    client_list.append(email_)
    change_id = client_list.pop(0)
    client_list.append(change_id)

    return client_list


if __name__ == '__main__':
    conn = psycopg2.connect(database='sql_hw5', user='postgres', password='r3l0ATprogef3w_+')
    with conn.cursor() as cur:
        create_db(conn)

        add_client(conn, first_name='Егор', last_name='Субботин', email='subbotin@yandex.ru', phones=phones_list_1)
        add_client(conn, first_name='Александр', last_name='Захаров', email='zaharov@yandex.ru', phones=89524633476)
        add_client(conn, first_name='Василий', last_name='Морозов', email='morozov@yandex.ru')

        add_phone(conn, client_id=2, phone=12344633476)
        add_phone(conn, client_id=3, phone=phones_list_2)

        change_client(conn, client_id=1, first_name='неЕгор', phones=phones_list_3)
        change_client(conn, client_id=2, last_name='неЗахаров', email='NEzaharov@yandex.ru')

        delete_phone(conn, client_id=2, phone=89524633476)

        delete_client(conn, client_id=1)

        print(find_client(conn, first_name='Александр'))
        print(find_client(conn, last_name='Морозов'))
        print(find_client(conn, email='morozov@yandex.ru'))
        print(find_client(conn, phone=123433333678901))

        cur.execute("""
            SELECT * from Clients;
                """)
        print(f'Clients: {cur.fetchall()}')

        cur.execute("""
            SELECT * from Numbers;
                """)
        print(f'Numbers: {cur.fetchall()}')

        cur.execute("""
            SELECT * from ClientsNumbers;
                """)
        print(f'ClientsNumbers: {cur.fetchall()}')

    cur.close()
    conn.close()
