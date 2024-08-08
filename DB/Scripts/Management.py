import json
import sqlite3


# התחברות למסד הנתונים (או יצירת מסד נתונים חדש אם הוא לא קיים)
# התחברות למסד הנתונים (או יצירת מסד נתונים חדש אם הוא לא קיים)
# conn = sqlite3.connect('database.db')
# cursor = conn.cursor()

# יצירת טבלה חדשה
# cursor.execute('''
#     CREATE TABLE Management (
#         name TEXT NOT NULL,
#         id_number TEXT NOT NULL,
#         metadata TEXT
#     )
# ''')

# התחייבות לשינויים וסגירת החיבור
# conn.commit()
# conn.close()


# def connect_to_db(db_name):
#     try:
#         conn = sqlite3.connect(db_name)
#         return conn
#     except sqlite3.Error as err:
#         print(f"Error connecting to the database: {err}")
#         return None


def delete_from_Management(name, id_number, conn=None):
    """
      פונקציה למחיקת רשומה מהטבלה Management לפי שם ומספר מזהה

      Args:
      class_name (str): שם הרשומה למחיקה
      object_id (str): מספר מזהה של הרשומה למחיקה

      Returns:
      bool: True אם המחיקה הצליחה, False אחרת
    """
    # conn = connect_to_db('database.db')
    # if conn is None:
    #     return
    conn_is_None = False
    if not conn:
        conn = sqlite3.connect('database.db')
        conn_is_None = True
    cursor = conn.cursor()
    cursor.execute('''
          DELETE FROM Management
          WHERE class_name = ? AND object_id = ?
      ''', (name, id_number))

    conn.commit()
    if conn_is_None:
        conn.close()


def update_metadata(name_condition, id_condition, key, new_value, conn=None, is_update_parameter=False,
                    name_of_parameter=None):
    print("conn_update_metadata:", conn)
    conn_is_None = False
    if not conn:
        conn = sqlite3.connect('database.db')
        conn_is_None = True
    cursor = conn.cursor()
    try:
        print("metadata:", name_condition, id_condition)
        # שליפת הרשומות שעונות על התנאי
        cursor.execute('''
            SELECT class_name, object_id, metadata
            FROM Management
            WHERE class_name = ? AND object_id = ?
        ''', (name_condition, id_condition))

        records = cursor.fetchall()
        for record in records:
            name, id_number, metadata = record
            metadata_dict = json.loads(metadata)

            # עדכון התכונה ב-metadata
            if is_update_parameter is False:
                metadata_dict[key] = new_value
            else:
                # parameter = [p for p in metadata_dict['parameters'] if p['ParameterName'] == name_of_parameter][0]
                # print(parameter)
                parameter = None
                count = -1
                for p in metadata_dict['parameters']:
                    if p['ParameterName'] == name_of_parameter:
                        parameter = p
                        count += 1
                if parameter is not None:
                    parameter[key] = new_value
                    # update_parameters = json.dumps(attributes_of_parameter)
                    metadata_dict['parameters'][count] = parameter
                # print(metadata_dict)
            # המרה בחזרה למחרוזת JSON
            updated_metadata = json.dumps(metadata_dict)
            print("metadata:", updated_metadata, name, id_number)
            # עדכון הרשומה בטבלה
            cursor.execute('''
                UPDATE Management
                SET metadata = ?
                WHERE class_name = ? AND object_id = ?
            ''', (updated_metadata, name, id_number))
        conn.commit()
    except sqlite3.Error as e:
        raise RuntimeError(f"Error updating metadata: {e}")
    finally:
        if conn_is_None:
            conn.close()


def execute_query(db_name, query, params, conn=None):
    conn_is_None = False
    if not conn:
        conn = sqlite3.connect(db_name)
        conn_is_None = True
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    print(query)
    if params:

        c.execute(query, params)
    else:
        c.execute(query)

    res = c.fetchall()

    conn.commit()
    conn.close()
    return res


def insert_into_management_table(class_name, object_id, metadata, conn=None):
    conn_is_None = False
    if not conn:
        conn = sqlite3.connect('database.db')
        conn_is_None = True
    c = conn.cursor()
    execute_query('database.db',
                  '''CREATE TABLE IF NOT EXISTS object_management (object_id TEXT PRIMARY KEY, class_name TEXT NOT NULL, metadata TEXT)''',
                  None)
    c.execute('''
          INSERT INTO Management (class_name, object_id, metadata)
          VALUES (?, ?, ?)
          ''', (class_name, object_id, metadata))
    conn.commit()
    if conn_is_None:
        conn.close()
