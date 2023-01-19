import mysql.connector

# connect with the mysql fragment site

mysqlFragment = mysql.connector.connect(
    host="localhost",
    user="distributed",
    password="password"
)

# connect to the centralized database
localData = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="Chinook"
)

mysqlFragment_cursor = mysqlFragment.cursor()


def init_fragment():
    mysqlFragment_cursor.execute("CREATE DATABASE IF NOT EXISTS chinooksite")
    mysqlFragment_cursor.execute("USE chinooksite")

    # create fragment table for customers who are from the USA or Canada
    mysqlFragment_cursor.execute("""
        CREATE TABLE IF NOT EXISTS `Customer` (
            `CustomerId` int NOT NULL,
            `FirstName` varchar(40) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
            `LastName` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
            `Company` varchar(80) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
            `Address` varchar(70) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
            `City` varchar(40) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
            `State` varchar(40) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
            `Country` varchar(40) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
            `PostalCode` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
            `Phone` varchar(24) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
            `Fax` varchar(24) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
            `Email` varchar(60) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
            `SupportRepId` int DEFAULT NULL,
            PRIMARY KEY (`CustomerId`)
            )
    """)

    # m1: Customers who are from the USA or Canada(Primary Horizontal Fragmentation)
    localData_cursor = localData.cursor(buffered=True)
    localData_query = 'SELECT * FROM Customer Where Country = "USA" OR Country = "Canada"'

    print("Primary Fragmentation m1")
    print("")
    print("Minterm fragment fetched from localhost")
    localData_cursor.execute(localData_query)
    local_customers_query_results = localData_cursor.fetchall()
    print(local_customers_query_results)
    print("")

    # Inserting the data into site table
    mysqlFragment_clear = "DELETE FROM Customer"
    mysqlFragment_cursor.execute(mysqlFragment_clear)
    mysqlFragment_insert_customers_sql = """
    INSERT INTO `Customer`
    (`CustomerId`,`FirstName`,`LastName`,`Company`,`Address`,`City`,`State`,`Country`,`PostalCode`,`Phone`,`Fax`,`Email`,`SupportRepId`)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    mysqlFragment_cursor.executemany(mysqlFragment_insert_customers_sql, local_customers_query_results)
    mysqlFragment.commit();

    mysqlFragment_cursor.execute('SHOW TABLES')

    for x in mysqlFragment_cursor:
        print(x)

def customer_vertical_fragment():
    mysqlFragment_cursor.execute("CREATE DATABASE IF NOT EXISTS chinooksite")
    mysqlFragment_cursor.execute("USE chinooksite")

    # create fragment table for customers who are from the USA or Canada
    mysqlFragment_cursor.execute("""
        CREATE TABLE IF NOT EXISTS `Customer_Vertical_Names` (
            `CustomerId` int NOT NULL,
            `FirstName` varchar(40) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
            `LastName` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
            PRIMARY KEY (`CustomerId`)
            )
    """)

    # m1: Customers who are from the USA or Canada(Primary Horizontal Fragmentation)
    localData_cursor = localData.cursor(buffered=True)
    localData_query = 'SELECT CustomerId, FirstName, LastName FROM Customer'

    print("Vertical Fragmentation m1")
    print("")
    print("Fragment fetched from localhost")
    localData_cursor.execute(localData_query)
    local_customers_query_results = localData_cursor.fetchall()
    print(local_customers_query_results)
    print("")

    # Inserting the data into site table
    mysqlFragment_clear = "DELETE FROM Customer_Vertical_Names"
    mysqlFragment_cursor.execute(mysqlFragment_clear)
    mysqlFragment_insert_customers_sql = """
    INSERT INTO `Customer_Vertical_Names`
    (`CustomerId`,`FirstName`,`LastName`)
    VALUES(%s,%s,%s);
    """
    mysqlFragment_cursor.executemany(mysqlFragment_insert_customers_sql, local_customers_query_results)
    mysqlFragment.commit()


init_fragment();
customer_vertical_fragment()
