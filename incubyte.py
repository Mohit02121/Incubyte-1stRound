import sqlite3

def create_country_table(country_name):
    #
    ##For Each Country Table is created in database
    #
    conn = sqlite3.connect("inubyte.db")
    query = "CREATE TABLE IF NOT EXISTS "+country_name+" (Customer_Name VARCHAR(255), Customer_Id VARCHAR(18) NOT NULL, Customer_Open_Date DATE NOT NULL, Last_Consulted_Date DATE NOT NULL, Vaccination_Type CHAR(5),DR_Name VARCHAR(20), State CHAR(5), Date_Of_Birth DATE, Active_Customer CHAR(1))"
    cur = conn.cursor()
    cur.execute(query)
    print("For "+country_name+" country separate table is created successfully...")


def distribute_data_into_tables():
    #
    ##Data from temp table is inserted into separate country tables.
    #
    conn = sqlite3.connect("inubyte.db")
    query = "SELECT * FROM temp"
    cur = conn.cursor()
    result = cur.execute(query).fetchall()
    for res in result:
        res = list(res)
        country = res.pop(-3)
        res = tuple(res)
        query = "INSERT INTO "+country+" VALUES(?,?,?,?,?,?,?,?,?)"
        cur.execute(query,res)
        conn.commit()
        #print(country,res)
    print("Data is saved in country tables")
    cur.execute("DROP TABLE IF EXISTS temp ")
    conn.commit()
    print("Temp table is deleted")


def save(Customer_Name, Customer_ID, Customer_Open_Date, Last_Cunsulted_Date, Vaccination_Type, State, Country, Post_Code, Date_Of_Birth, Active_Customer):
    #
    ##Row is saved into temp table..
    #
    conn = sqlite3.connect("inubyte.db")
    query = '''INSERT INTO temp values(?,?,?,?,?,?,?,?,?,?)'''
    cur = conn.cursor()
    data = (Customer_Name, Customer_ID, Customer_Open_Date, Last_Cunsulted_Date, Vaccination_Type, State, Country, Post_Code, Date_Of_Birth, Active_Customer)
    cur.execute(query,data)
    conn.commit()
    


def file_to_db():
    #
    ##Reading from file and sending data to temp table
    #
    try:
        conn = sqlite3.connect("inubyte.db")
        query = "CREATE TABLE IF NOT EXISTS temp(Customer_Name VARCHAR(255), Customer_Id VARCHAR(18) NOT NULL, Customer_Open_Date DATE NOT NULL, Last_Consulted_Date DATE NOT NULL, Vaccination_Type CHAR(5),DR_Name VARCHAR(20), State CHAR(5), Country CHAR(5), Date_Of_Birth DATE, Active_Customer CHAR(1))"
        cur = conn.cursor()
        cur.execute(query)
    except:
        print("Temp Table is not created")


    file1 = open("data.txt","r")
    lines = file1.readlines()
    for line in lines:
        row = line.split("|")[1:]
        Customer_Name = row[1]
        Customer_ID = row[2]
        Customer_Open_Date = row[3]
        Last_Cunsulted_Date = row[4]
        Vaccination_Type = row[5]
        State = row[6]
        Country = row[7]
        Post_Code = row[8]
        Date_Of_Birth = row[9]
        Active_Customer = row[10]

        save(Customer_Name, Customer_ID, Customer_Open_Date, Last_Cunsulted_Date, Vaccination_Type, State, Country, Post_Code, Date_Of_Birth, Active_Customer[0])
        ##print(Customer_Name, Customer_ID, Customer_Open_Date, Last_Cunsulted_Date, Vaccination_Type, State, Country, Post_Code, Date_Of_Birth, Active_Customer)
    print("DATA IS SAVED TO TEMP TABLE")



def all_country():
    #
    ## Retrieve All the country from temp table
    #
    countries = []
    conn = sqlite3.connect("inubyte.db")
    query = "SELECT DISTINCT COUNTRY FROM temp"
    cur = conn.cursor()
    res = cur.execute(query).fetchall()
    for r in res:
        create_country_table(r[0])




if __name__ == "__main__":
    #
    ## Main function to execute all function.
    #
    file_to_db()
    all_country()
    distribute_data_into_tables()

