import sqlite3 
import datetime
from sqlite3 import Error

create_employees_script = \
"""CREATE TABLE employees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    [name] TEXT,
    salary real,
    department TEXT,
    position TEXT,
    hireDate TEXT
)
"""

add_employees_script = \
"""INSERT INTO employees ([name], salary, department, position, hireDate)
VALUES ('John', 700, 'HR', 'Manager', '2017-01-04'),
('Andrew', 800, 'IT', 'Tech', '2018-02-06');
"""

update_employess_script = \
""" UPDATE employees 
SET [name] = 'Rogers'
WHERE id = 1
"""

select_all_employees_script = \
"""SELECT * FROM employees
"""

select_where_employees_script = \
"""SELECT id, [name] FROM employess
WHERE salary > 800.0
"""

select_tables_script =  \
"""SELECT name FROM sqlite_master 
WHERE type = 'table'
"""

create_projects_script = \
""" CREATE TABLE IF NOT EXISTS projects (
id INT,
[name] TEXT
)
"""

projects_insert_data = [
    (1, "Ridesharing"), 
    (2, "Water Purifying"), 
    (3, "Forensics"), 
    (4, "Botany")
]

drop_projects_script = \
"""DROP TABLE IF EXISTS projects 
"""

drop_employees_script = \
"""DROP TABLE IF EXISTS employees
"""

create_assignments_script = \
"""CREATE TABLE IF NOT EXIST assignments(
id INT,
[name] TEXT,
[date] DATE
)
"""

assignments_data = [
    (1, "Ridesharing", datetime.date(2017, 1, 2)), 
    (2, "Water Purifying", datetime.date(2018, 3, 4))
    ]

create_table_streets_script = \
"""
CREATE TABLE IF NOT EXISTS streets (
id INTEGER PRIMARY KEY AUTOINCREMENT,
[name] TEXT,
last_building_address INT
)
"""

streets_insert_data = [
    ( "Sudoremontnaya", 52),
    ( "Mira", 96),
    ( "Karla Marksa", 127),
    ("Severnaya", 46),
    ( "Sovetskiy Prospect", 172)

]



def connect (db_name : str):

    try:
        connection = sqlite3.connect(db_name)
        return connection
    except Error:
        print(Error)

def create_employees(connection_to_db : sqlite3.Connection):
    cursor = connection_to_db.cursor()
    cursor.execute(create_employees_script)
    connection_to_db.commit()

def populate_employees(connection_to_db : sqlite3.Connection):
    cursor = connection_to_db.cursor()
    cursor.execute(add_employees_script)
    connection_to_db.commit()

def update_employees(connection_to_db : sqlite3.Connection):
    cursor = connection_to_db.cursor()
    cursor.execute(update_employess_script)
    connection_to_db.commit()

def select_all_employees(connection_to_db : sqlite3.Connection):
    cursor = connection_to_db.cursor()
    cursor.execute(select_all_employees_script)
    employees = cursor.fetchall()
    for row in employees :
        print(row)

def select_where_employees(connection_to_db : sqlite3.Connection):
    cursor = connection_to_db.cursor()
    cursor.execute(select_where_employees_script)
    employees = cursor.fetchall()
    for row in employees :
        print(row)

def drop_employees(connection_to_db : sqlite3.Connection):
    cursor = connection_to_db.cursor()
    cursor.execute(drop_employees_script)
    connection_to_db.commit()

def create_and_populate_projects(connection_to_db : sqlite3.Connection):
    cursor = connection_to_db.cursor()
    cursor.execute(create_projects_script)
    cursor.executemany("INSERT INTO projects([id],[name]) VALUES(?,?)", projects_insert_data)
    connection_to_db.commit()

def create_and_populate_assignments(connection_to_db : sqlite3.Connection):
    cursor = connection_to_db.cursor()
    cursor.execute(create_assignments_script)
    cursor.executemany("INSERT INTO assignments VALUES (?,?,?)", assignments_data)
    connection_to_db.commit()

def create_and_populate_streets(connection_to_db : sqlite3.Connection):
    cursor = connection_to_db.cursor()
    cursor.execute(create_table_streets_script)
    cursor.executemany("INSERT INTO streets VALUES (NULL, ?, ?)", streets_insert_data)
    connection_to_db.commit()

def select_streets(connection_to_db : sqlite3.Connection) :
    cursor = connection_to_db.cursor()
    cursor.execute("SELECT * FROM streets")
    streets = cursor.fetchall()
    for street in streets :
        print(street)

def drop_streets (connection_to_db : sqlite3.Connection) :
    cursor = connection_to_db.cursor()
    cursor.execute("DROP TABLE IF EXISTS streets")


connection = connect("test_db.db")
drop_employees(connection)
create_employees(connection)
populate_employees(connection)
select_all_employees(connection)
update_employees(connection)
select_all_employees(connection)
connection.close()

connection = connect("cities_db.db")
drop_streets(connection)
create_and_populate_streets(connection)
select_streets(connection)
connection.close()


