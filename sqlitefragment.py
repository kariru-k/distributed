import mysql.connector
import sqlite3

# connect with the mysql fragment site

sqlitefragment = sqlite3.connect('distributedsqlite')

sqlitefragment_cursor = sqlitefragment.cursor()

# connect to the centralized database
localData = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="Chinook"
)


def non_northamerican_fragment():

    # create fragment table for customers who are not from the USA or Canada
    sqlitefragment_cursor.execute("""
        CREATE TABLE IF NOT EXISTS `Customer` (
            `CustomerId` INTEGER NOT NULL,
            `FirstName` NVARCHAR(40) NOT NULL,
            `LastName` NVARCHAR(20)  NOT NULL,
            `Company` NVARCHAR(80)  DEFAULT NULL,
            `Address` NVARCHAR(70)  DEFAULT NULL,
            `City` NVARCHAR(40)  DEFAULT NULL,
            `State` NVARCHAR(40)  DEFAULT NULL,
            `Country` NVARCHAR(40)  DEFAULT NULL,
            `PostalCode` NVARCHAR(10)  DEFAULT NULL,
            `Phone` NVARCHAR(24)  DEFAULT NULL,
            `Fax` NVARCHAR(24)  DEFAULT NULL,
            `Email` NVARCHAR(60)  NOT NULL,
            `SupportRepId` INTEGER DEFAULT NULL,
            PRIMARY KEY (`CustomerId`)
            )
    """)

    # m1: Customers who are not from the USA or Canada(Primary Horizontal Fragmentation)
    localData_cursor = localData.cursor(buffered=True)
    localData_query = 'SELECT * FROM Customer WHERE NOT (Country = "USA" OR Country = "Canada")'

    print("Primary Fragmentation m2: Customers not from the United States Or Canada")
    print("")
    print("Minterm fragment fetched from localhost")
    localData_cursor.execute(localData_query)
    local_customers_query_results = localData_cursor.fetchall()
    print(local_customers_query_results)
    print("")

    # Inserting the data into site table
    sqlitefragment_clear = "DELETE FROM Customer"
    sqlitefragment_cursor.execute(sqlitefragment_clear)
    sqlitefragment_insert_customers_sql = """
    INSERT INTO `Customer`
    (`CustomerId`,`FirstName`,`LastName`,`Company`,`Address`,`City`,`State`,`Country`,`PostalCode`,`Phone`,`Fax`,`Email`,`SupportRepId`)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);
    """
    sqlitefragment_cursor.executemany(sqlitefragment_insert_customers_sql, local_customers_query_results)
    sqlitefragment.commit()


def company_customers():
    sqlitefragment_cursor.execute("""
                CREATE TABLE IF NOT EXISTS `CompanyCustomers` (
                    `CustomerId` INTEGER NOT NULL,
                    `FirstName` NVARCHAR(40) NOT NULL,
                    `LastName` NVARCHAR(20)  NOT NULL,
                    `Company` NVARCHAR(80)  DEFAULT NULL,
                    `Address` NVARCHAR(70)  DEFAULT NULL,
                    `City` NVARCHAR(40)  DEFAULT NULL,
                    `State` NVARCHAR(40)  DEFAULT NULL,
                    `Country` NVARCHAR(40)  DEFAULT NULL,
                    `PostalCode` NVARCHAR(10)  DEFAULT NULL,
                    `Phone` NVARCHAR(24)  DEFAULT NULL,
                    `Fax` NVARCHAR(24)  DEFAULT NULL,
                    `Email` NVARCHAR(60)  NOT NULL,
                    `SupportRepId` INTEGER DEFAULT NULL,
                    PRIMARY KEY (`CustomerId`)
                    )
            """)

    # m1: Customers who are from a company(Primary Horizontal Fragmentation)
    localData_cursor = localData.cursor(buffered=True)
    localData_query = 'SELECT * FROM Customer WHERE Company IS NOT NULL'

    print("Primary Fragmentation m1: Customers are from a company")
    print("")
    print("Minterm fragment fetched from localhost")
    localData_cursor.execute(localData_query)
    local_customers_query_results = localData_cursor.fetchall()
    print(local_customers_query_results)
    print("")

    # Inserting the data into site table
    sqlitefragment_clear = "DELETE FROM CompanyCustomers"
    sqlitefragment_cursor.execute(sqlitefragment_clear)
    sqlitefragment_insert_customers_sql = """
        INSERT INTO `CompanyCustomers`
        (`CustomerId`,`FirstName`,`LastName`,`Company`,`Address`,`City`,`State`,`Country`,`PostalCode`,`Phone`,`Fax`,`Email`,`SupportRepId`)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);
        """
    sqlitefragment_cursor.executemany(sqlitefragment_insert_customers_sql, local_customers_query_results)
    sqlitefragment.commit()


#Derived Horizontal Fragmentation Of Invoices with respect to Customer fragment
def company_customers_invoices_fragments():
    # create fragment table for customer invoices from customers who aren't attached to a company
    sqlitefragment_cursor.execute("""
    CREATE TABLE IF NOT EXISTS 'CompanyClientsInvoice'
    (
    'InvoiceId' INTEGER  NOT NULL,
    'CustomerId' INTEGER  NOT NULL,
    'InvoiceDate' DATETIME  NOT NULL,
    'BillingAddress' NVARCHAR(70),
    'BillingCity' NVARCHAR(40),
    'BillingState' NVARCHAR(40),
    'BillingCountry' NVARCHAR(40),
    'BillingPostalCode' NVARCHAR(10),
    'Total' INTEGER,
    CONSTRAINT 'PK_Invoice' PRIMARY KEY  ('InvoiceId')
)
""")

    localData_cursor = localData.cursor(buffered=True)
    localData_query = """
        SELECT InvoiceId, Invoice.CustomerId, InvoiceDate, BillingAddress, BillingCity, BillingState, BillingCountry, BillingPostalCode, Total
        FROM Customer, Invoice
        Where Customer.Company IS NOT NULL
        AND Customer.CustomerId = Invoice.CustomerId
        """
    print("Derived Horizontal Fragmentation")
    print("")
    print("Minterm fragment fetched from localhost")
    localData_cursor.execute(localData_query)
    local_customers_query_results = localData_cursor.fetchall()


    for i in range(len(local_customers_query_results)):
        local_customers_query_results[i] = list(local_customers_query_results[i])
        local_customers_query_results[i][8] = str(local_customers_query_results[i][8])
        local_customers_query_results[i] = tuple(local_customers_query_results[i])


    print(local_customers_query_results)
    print("")

    # Inserting the data into site table
    mysqlFragment_clear = "DELETE FROM CompanyClientsInvoice"
    sqlitefragment_cursor.execute(mysqlFragment_clear)
    mysqlFragment_insert_customers_sql = """
            INSERT INTO CompanyClientsInvoice
            (InvoiceId, CustomerId, InvoiceDate, BillingAddress, BillingCity, BillingState, BillingCountry, BillingPostalCode, Total)
            VALUES(?,?,?,?,?,?,?,?,?);
            """
    sqlitefragment_cursor.executemany(mysqlFragment_insert_customers_sql, local_customers_query_results)
    sqlitefragment.commit()

    def non_northamerican_fragment():
        # create fragment table for customers who are not from the USA or Canada
        sqlitefragment_cursor.execute("""
            CREATE TABLE IF NOT EXISTS `Customer` (
                `CustomerId` INTEGER NOT NULL,
                `FirstName` NVARCHAR(40) NOT NULL,
                `LastName` NVARCHAR(20)  NOT NULL,
                `Company` NVARCHAR(80)  DEFAULT NULL,
                `Address` NVARCHAR(70)  DEFAULT NULL,
                `City` NVARCHAR(40)  DEFAULT NULL,
                `State` NVARCHAR(40)  DEFAULT NULL,
                `Country` NVARCHAR(40)  DEFAULT NULL,
                `PostalCode` NVARCHAR(10)  DEFAULT NULL,
                `Phone` NVARCHAR(24)  DEFAULT NULL,
                `Fax` NVARCHAR(24)  DEFAULT NULL,
                `Email` NVARCHAR(60)  NOT NULL,
                `SupportRepId` INTEGER DEFAULT NULL,
                PRIMARY KEY (`CustomerId`)
                )
        """)

        # m1: Customers who are not from the USA or Canada(Primary Horizontal Fragmentation)
        localData_cursor = localData.cursor(buffered=True)
        localData_query = 'SELECT * FROM Customer WHERE NOT (Country = "USA" OR Country = "Canada")'

        print("Primary Fragmentation m2: Customers not from the United States Or Canada")
        print("")
        print("Minterm fragment fetched from localhost")
        localData_cursor.execute(localData_query)
        local_customers_query_results = localData_cursor.fetchall()
        print(local_customers_query_results)
        print("")

        # Inserting the data into site table
        sqlitefragment_clear = "DELETE FROM Customer"
        sqlitefragment_cursor.execute(sqlitefragment_clear)
        sqlitefragment_insert_customers_sql = """
        INSERT INTO `Customer`
        (`CustomerId`,`FirstName`,`LastName`,`Company`,`Address`,`City`,`State`,`Country`,`PostalCode`,`Phone`,`Fax`,`Email`,`SupportRepId`)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);
        """
        sqlitefragment_cursor.executemany(sqlitefragment_insert_customers_sql, local_customers_query_results)
        sqlitefragment.commit()


#Customer Vertical Fragment Two
def customer_vertical_fragment():

    # create fragment table for customers who are not from the USA or Canada
    sqlitefragment_cursor.execute("""
        CREATE TABLE IF NOT EXISTS `Customer_Vertical_Two` (
            `CustomerId` INTEGER NOT NULL,
            `Phone` NVARCHAR(24)  DEFAULT NULL,
            `Fax` NVARCHAR(24)  DEFAULT NULL,
            `Email` NVARCHAR(60)  NOT NULL,
            PRIMARY KEY (`CustomerId`)
            )
    """)

    # m1: Customers who are not from the USA or Canada(Primary Horizontal Fragmentation)
    localData_cursor = localData.cursor(buffered=True)
    localData_query = 'SELECT CustomerId, Phone, Fax, Email FROM Customer'

    print("Vertical Fragmentation m2: Customers and their contacts")
    print("")
    print("Minterm fragment fetched from localhost")
    localData_cursor.execute(localData_query)
    local_customers_query_results = localData_cursor.fetchall()
    print(local_customers_query_results)
    print("")

    # Inserting the data into site table
    sqlitefragment_clear = "DELETE FROM Customer_Vertical_Two"
    sqlitefragment_cursor.execute(sqlitefragment_clear)
    sqlitefragment_insert_customers_sql = """
    INSERT INTO `Customer_Vertical_Two`
    (`CustomerId`,`Phone`,`Fax`,`Email`)
    VALUES(?,?,?,?);
    """
    sqlitefragment_cursor.executemany(sqlitefragment_insert_customers_sql, local_customers_query_results)
    sqlitefragment.commit()



non_northamerican_fragment()
company_customers()
company_customers_invoices_fragments()
customer_vertical_fragment()