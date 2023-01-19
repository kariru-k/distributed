import psycopg2
import mysql.connector

connection = psycopg2.connect(
    host='192.168.5.229',
    user='postgres',
    password='password',
    database='distributedFragment'
)

localData = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="Chinook"
)

connection_cursor = connection.cursor()


def no_company_fragment():
    # create fragment table for customers who are from the USA or Canada
    connection_cursor.execute("""
        CREATE TABLE IF NOT EXISTS nonCompanyCustomers(
            CustomerId int NOT NULL,
            FirstName varchar(40) NOT NULL,
            LastName varchar(20)  NOT NULL,
            Company varchar(80)  DEFAULT NULL,
            Address varchar(70)  DEFAULT NULL,
            City varchar(40)  DEFAULT NULL,
            State varchar(40)  DEFAULT NULL,
            Country varchar(40)  DEFAULT NULL,
            PostalCode varchar(10)  DEFAULT NULL,
            Phone varchar(24)  DEFAULT NULL,
            Fax varchar(24) DEFAULT NULL,
            Email varchar(60) NOT NULL,
            SupportRepId int DEFAULT NULL,
            PRIMARY KEY (CustomerId)
        )
""")

    # m1: Customers who are from the USA or Canada(Primary Horizontal Fragmentation)
    localData_cursor = localData.cursor(buffered=True)
    localData_query = 'SELECT * FROM Customer Where Company IS NULL'

    print("Primary Fragmentation m2")
    print("")
    print("Minterm fragment fetched from localhost")
    localData_cursor.execute(localData_query)
    local_customers_query_results = localData_cursor.fetchall()
    print(local_customers_query_results)
    print("")

    # Inserting the data into site table
    mysqlFragment_clear = "DELETE FROM NonCompanyCustomers"
    connection_cursor.execute(mysqlFragment_clear)
    mysqlFragment_insert_customers_sql = """
        INSERT INTO NonCompanyCustomers
        (CustomerId,FirstName,LastName,Company,Address,City,State,Country,PostalCode,Phone,Fax,Email,SupportRepId)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """
    connection_cursor.executemany(mysqlFragment_insert_customers_sql, local_customers_query_results)
    connection.commit()


# Derived Horizontal Fragmentation of Invoices Using The Non Company Fragment above
def no_company_invoice_fragment():
    # create fragment table for customer invoices from customers who aren't attached to a company
    connection_cursor.execute("""
        CREATE TABLE IF NOT EXISTS NonCompanyCustomersInvoice (
        InvoiceId int NOT NULL,
        CustomerId int NOT NULL,
        InvoiceDate DATE NOT NULL,
        BillingAddress varchar(70)  DEFAULT NULL,
        BillingCity varchar(40)  DEFAULT NULL,
        BillingState varchar(40)  DEFAULT NULL,
        BillingCountry varchar(40)  DEFAULT NULL,
        BillingPostalCode varchar(10)  DEFAULT NULL,
        Total decimal(10,2) NOT NULL,
        PRIMARY KEY (InvoiceId),
        CONSTRAINT FK_InvoiceCustomerId FOREIGN KEY (CustomerId) REFERENCES nonCompanyCustomers (CustomerId) ON DELETE CASCADE
    ) 
""")

    localData_cursor = localData.cursor(buffered=True)
    localData_query = """
    SELECT InvoiceId, Invoice.CustomerId, InvoiceDate, BillingAddress, BillingCity, BillingState, BillingCountry, BillingPostalCode, Total
    FROM Customer, Invoice
    Where Customer.Company IS NULL
    AND Customer.CustomerId = Invoice.CustomerId
    """
    print("Derived Horizontal Fragmentation")
    print("")
    print("Minterm fragment fetched from localhost")
    localData_cursor.execute(localData_query)
    local_customers_query_results = localData_cursor.fetchall()
    print(local_customers_query_results)
    print("")

    # Inserting the data into site table
    mysqlFragment_clear = "DELETE FROM NonCompanyCustomersInvoice"
    connection_cursor.execute(mysqlFragment_clear)
    mysqlFragment_insert_customers_sql = """
        INSERT INTO NonCompanyCustomersInvoice
        (InvoiceId, CustomerId, InvoiceDate, BillingAddress, BillingCity, BillingState, BillingCountry, BillingPostalCode,Total)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """
    connection_cursor.executemany(mysqlFragment_insert_customers_sql, local_customers_query_results)
    connection.commit()


def customers_vertical_fragment():
    # create fragment table for customers who are from the USA or Canada
    connection_cursor.execute("""
        CREATE TABLE IF NOT EXISTS Customers_Vertical_Fragment(
            CustomerId int NOT NULL,
            Address varchar(70)  DEFAULT NULL,
            City varchar(40)  DEFAULT NULL,
            State varchar(40)  DEFAULT NULL,
            Country varchar(40)  DEFAULT NULL,
            PRIMARY KEY (CustomerId)
        )
""")

    # m1: Customers who are from the USA or Canada(Primary Horizontal Fragmentation)
    localData_cursor = localData.cursor(buffered=True)
    localData_query = 'SELECT CustomerId, Address, City, State, Country FROM Customer'

    print("Vertical Fragmentation m3")
    print("")
    print("Minterm fragment fetched from localhost")
    localData_cursor.execute(localData_query)
    local_customers_query_results = localData_cursor.fetchall()
    print(local_customers_query_results)
    print("")

    # Inserting the data into site table
    mysqlFragment_clear = "DELETE FROM Customers_Vertical_Fragment"
    connection_cursor.execute(mysqlFragment_clear)
    mysqlFragment_insert_customers_sql = """
        INSERT INTO Customers_Vertical_Fragment
        (CustomerId,Address,City,State,Country)
        VALUES(%s,%s,%s,%s,%s);
        """
    connection_cursor.executemany(mysqlFragment_insert_customers_sql, local_customers_query_results)
    connection.commit()


no_company_fragment()
no_company_invoice_fragment()
customers_vertical_fragment()
