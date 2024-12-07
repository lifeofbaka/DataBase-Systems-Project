import sqlite3
import pandas as pd
# Ramses Loaces Project PArt 3
# DataBase Systems Fall 2024


# Create database file
db_connect = sqlite3.connect('Clinic.db')

# Instantiate cursor object for executing queries
cursor = db_connect.cursor()

cursor.execute("PRAGMA foreign_keys = 1")
# String variable for passing queries to cursor

# https://www.sqlite.org/docs.html
# https://www.sqlite.org/datatype3.html

cursor.execute("DROP TABLE IF EXISTS Clinc")
cursor.execute("DROP TABLE IF EXISTS Staff")
cursor.execute("DROP TABLE IF EXISTS Owner")
cursor.execute("DROP TABLE IF EXISTS Pet")
cursor.execute("DROP TABLE IF EXISTS Examination")

# a. Develop SQL code to create the entire database schema, reflecting the constraints
# identified in previous steps.

create_clinic_table = """
    CREATE TABLE IF NOT EXISTS Clinic (
        ClinicNo VARCHAR PRIMARY KEY,
        ClinicName VARCHAR NOT NULL,
        ClinicAddress VARCHAR NOT NULL,
        ClinicPhoneNumber VARCHAR UNIQUE,
        StaffNo VARCHAR,
        FOREIGN KEY (StaffNo) REFERENCES Staff(StaffNo) ON DELETE SET NULL,
        CHECK (ClinicNo LIKE 'C%'),
        CHECK (ClinicPhoneNumber LIKE '___-___-____')
    );
"""

create_staff_table = """
    CREATE TABLE IF NOT EXISTS Staff (
        StaffNo VARCHAR PRIMARY KEY,
        StaffName VARCHAR NOT NULL,
        StaffTelephoneNumber VARCHAR UNIQUE,
        StaffDOB DATE NOT NULL,
        Position CHAR NOT NULL,
        Salary INT NOT NULL,
        ClinicNo VARCHAR,
        FOREIGN KEY (ClinicNo) REFERENCES Clinic(ClinicNo),
        CHECK (StaffNo LIKE 'S%'),
        CHECK (StaffTelephoneNumber LIKE '___-___-____')
    );
"""

create_Owner_table = """
    CREATE TABLE IF NOT EXISTS Owner (
        OwnerNo VARCHAR PRIMARY KEY,
        OwnerName CHAR NOT NULL,
        OwnerAddress VARCHAR NOT NULL,
        OwnerTelephoneNumber VARCHAR UNIQUE,
        CHECK (OwnerNo LIKE 'O%'),
        CHECK (OwnerTelephoneNumber LIKE '___-___-____')

    );
"""

create_Pet_table = """
    CREATE TABLE IF NOT EXISTS Pet (
        PetNo VARCHAR PRIMARY KEY,
        PetName CHAR NOT NULL,
        PetDOB DATE NOT NULL,
        AnimalSpecies CHAR NOT NULL,
        BREED CHAR NOT NULL,
        Color CHAR NOT NULL,
        OwnerNo VARCHAR NOT NULL,
        ClinicNo VARCHAR NOT NULL,
        FOREIGN KEY (OwnerNo) REFERENCES Owner(OwnerNo),
        FOREIGN KEY (ClinicNo) REFERENCES Clinic(ClinicNo)
        CHECK (PetNo LIKE 'P%')
    );
"""

create_Examination_table = """
    CREATE TABLE IF NOT EXISTS Examination (
        ExamNo VARCHAR PRIMARY KEY,
        ChiefComplaint CHAR NOT NULL,
        Description CHAR NOT NULL,
        DateSeen DATE NOT NULL,
        ActionTaken CHAR NOT NULL,
        PetNo VARCHAR NOT NULL,
        StaffNo VARCHAR NOT NULL,
        FOREIGN KEY (PetNo) REFERENCES Pet(PetNo),
        FOREIGN KEY (StaffNo) REFERENCES Staff(StaffNo)
        CHECK (ExamNo LIKE 'E%')
    );
"""
# Trigger when adding new items to the examination table. Staff assigned to the examination should be part of
# the Clinic where the Pet is registered.
trigger_query = """
CREATE TRIGGER validate_examination
BEFORE INSERT ON Examination
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Staff and Pet must belong to the same clinic')
    WHERE (SELECT ClinicNo FROM Pet WHERE PetNo = NEW.PetNo) != 
          (SELECT ClinicNo FROM Staff WHERE StaffNo = NEW.StaffNo);
END;
"""

cursor.execute(create_clinic_table)
cursor.execute(create_staff_table)
cursor.execute(create_Owner_table)
cursor.execute(create_Pet_table)
cursor.execute(create_Examination_table)
cursor.execute(trigger_query)

# b. Create at least 5 tuples for each relation in your database.

insert_items_Clinic = """
    INSERT INTO Clinic (ClinicNo, ClinicName, ClinicAddress, ClinicPhoneNumber) VALUES
    ('C001', 'Peanuts Clinic', '2244 James Street', '123-045-6789'),
    ('C002', 'The Pet Clinic', '2222 Johns Street', '123-054-6669'),
    ('C003', 'Johns Clinic', '505 Miller Street', '123-663-6789'),
    ('C004', 'Pets Clinic', '123 Main Street', '123-455-6789'),
    ('C005', 'Mom and Pop Clinic', '2323 Jefferson Street', '123-005-6583');
    """

insert_items_Staff = """
    INSERT INTO Staff (StaffNo, StaffName, StaffTelephoneNumber, StaffDOB, Position, Salary, ClinicNo) VALUES 
    ('S001', 'John Doe', '123-456-7890', '1987-07-15', 'Manager', 65000, 'C001'),
    ('S002', 'Jenny Smith', '123-456-7440', '1995-10-01', 'Nurse', 57000, 'C001'),
    ('S003', 'Michael Johnson', '123-656-7880', '1989-08-17', 'Manager', 59000, 'C002'),
    ('S004', 'Mitchel Jenkins', '123-455-6810', '1988-05-15', 'Nurse', 59000, 'C002'),
    ('S005', 'John Doe', '123-456-7843', '1990-06-20', 'Manager', 55000, 'C003'),
    ('S006', 'Joy shores', '123-456-7391', '1996-04-11', 'Manager', 60000, 'C004'),
    ('S007', 'Ashley Jenkins', '123-735-6810', '1995-07-30', 'Nurse', 55000, 'C004'),
    ('S008', 'Samantha Dinkins', '123-636-7700', '1980-09-15', 'Manager', 55000, 'C005');
    """

insert_items_Owner = """
    INSERT INTO Owner (OwnerNo, OwnerName, OwnerAddress, OwnerTelephoneNumber) VALUES
    ('O101', 'Charlie Brown', '1770 James Street', '123-256-5760'),
    ('O102', 'Jackie Obrian', '945 James Street', '123-176-0009'),
    ('O103', 'Jack Matthews', '3045 Johns Street', '123-390-4050'),
    ('O104', 'Dan White', '1000 Miller Street', '123-324-6654'),
    ('O105', 'Tony Stark', '1570 Jefferson Street', '123-889-9650');
    """

insert_items_Pet = """
    INSERT INTO Pet (PetNo, PetName, PetDOB, AnimalSpecies, Breed, Color, OwnerNo, ClinicNo) VALUES
    ('P1001', 'Snoopy', '2023-08-10', 'Dog,', 'Beagle', 'White', 'O101', 'C001'), 
    ('P1002', 'Buddy', '2024-05-10', 'Dog,', 'Golden Retriever', 'Golden', 'O102', 'C002'),
    ('P1003', 'Lassie', '2022-10-10', 'Dog,', 'Rough Collie', 'Brown', 'O103', 'C004'),
    ('P1004', 'Lady', '2020-06-18', 'Dog,', 'American Cocker Spaniel', 'Golden', 'O104', 'C004'),
    ('P1005', 'Scooby Doo', '2019-03-18', 'Dog,', 'Great Dane', 'Brown', 'O105', 'C004');
    """

insert_items_Examination= """
    INSERT INTO Examination (ExamNo, ChiefComplaint, Description, DateSeen, ActionTaken, PetNo, StaffNo) VALUES
    ('E201', 'Ear Infection', 'Owner brought in dog with signs of ear infection', '2024-09-23', 'Prescribed Ear Drops', 'P1001', 'S001'),
    ('E202', 'Fleas', 'Owner brought in dog with severe itching', '2024-10-30', 'Prescribed Flea medication', 'P1003', 'S006'),
    ('E203', 'Ear Infection', 'Owner brought in dog with signs of ear infection', '2024-11-03', 'Prescribed Flea medication', 'P1002', 'S003'),
    ('E204', 'Fleas', 'Owner brought in dog with severe itching', '2024-11-30', 'Prescribed Flea medication', 'P1004', 'S006'),
    ('E205', 'Fleas', 'Owner brought in dog with severe itching', '2024-12-04', 'Prescribed Flea medication', 'P1005', 'S006');
    """
# run all queries and update staff in charge of the clinic
managers = {'C001': 'S001', 'C002': 'S003', 'C004': 'S006'}

cursor.execute(insert_items_Clinic)
cursor.execute(insert_items_Staff)
for key, value in managers.items():
    update_clinic = f"""
        UPDATE Clinic
        SET StaffNo = '{value}'
        WHERE ClinicNo = '{key}';
    """
    cursor.execute(update_clinic)
cursor.execute(insert_items_Owner)
cursor.execute(insert_items_Pet)
cursor.execute(insert_items_Examination)

# c. Develop the 5 SQL queries that correspond to 2c using embedded SQL.

# return all rows in tables
tables = ['Clinic', 'Staff', 'Owner', 'Pet', 'Examination']
for i in tables:
    query = f"""
        SELECT *
        FROM {i}
        """
    cursor.execute(query)
    column_names = [row[0] for row in cursor.description]

    # Fetch data and load into a pandas dataframe
    table_data = cursor.fetchall()
    df = pd.DataFrame(table_data, columns=column_names)
    # Examine dataframe
    print(df)
    print(df.columns)


# Check validity of all rows in tables
query = """
SELECT e.ExamNo, e.PetNo, e.StaffNo, p.ClinicNo AS PetClinic, s.ClinicNo AS StaffClinic
FROM Examination e
JOIN Pet p ON e.PetNo = p.PetNo
JOIN Staff s ON e.StaffNo = s.StaffNo
WHERE p.ClinicNo != s.ClinicNo;
"""
cursor.execute(query)
invalid_rows = cursor.fetchall()

if invalid_rows:
    print("Invalid rows detected:")
    for row in invalid_rows:
        print(row)
else:
    print("All examinations are valid.")
