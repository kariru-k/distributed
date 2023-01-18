import mysql.connector
import psycopg2
import sqlite3

mysqlFragment = mysql.connector.connect(
    host="localhost",
    user="distributed",
    password="password",
    database = "chinooksite"
)

mysqlFragment_cursor = mysqlFragment.cursor()

connection = psycopg2.connect(
    host='192.168.5.217',
    user='postgres',
    password='password',
    database='distributedFragment'
)

connection_cursor = connection.cursor()

sqlitefragment = sqlite3.connect('distributedsqlite')

sqlitefragment_cursor = sqlitefragment.cursor()


# Reconstruct the first primary horizontal fragments queries from MySQL and SQLite
def reconstruct_primary_horizontal_fragment_1():
    # Reconstruction of first primary horizontal fragmentation
    # Obtain data
    sqlitefragment_query = "SELECT * FROM Customer"
    mysqlFragment_query = "SELECT * FROM Customer"

    sqlitefragment_cursor.execute(sqlitefragment_query)
    sqlitefragment_queryresults = sqlitefragment_cursor.fetchall()
    print(sqlitefragment_queryresults)


    mysqlFragment_cursor.execute(mysqlFragment_query)
    mysqlFragment_queryresults = mysqlFragment_cursor.fetchall()
    print(mysqlFragment_queryresults)

    mysqlFragment_cursor.execute("""
            CREATE TABLE IF NOT EXISTS `ReconstructedCustomerFromCountries` (
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

    mysqlFragment_clear = "DELETE FROM ReconstructedCustomerFromCountries"
    mysqlFragment_cursor.execute(mysqlFragment_clear)
    mysqlFragment_insert_customers_sql = """
        INSERT INTO `ReconstructedCustomerFromCountries`
        (`CustomerId`,`FirstName`,`LastName`,`Company`,`Address`,`City`,`State`,`Country`,`PostalCode`,`Phone`,`Fax`,`Email`,`SupportRepId`)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """
    mysqlFragment_cursor.executemany(mysqlFragment_insert_customers_sql, sqlitefragment_queryresults)
    mysqlFragment_cursor.executemany(mysqlFragment_insert_customers_sql, mysqlFragment_queryresults)
    mysqlFragment.commit()

def reconstruct_primary_horizontal_fragment_2():
    # Reconstruction of first primary horizontal fragmentation
    # Obtain data
    sqlitefragment_query = "SELECT * FROM CompanyCustomers"
    connection_cursor_query = "SELECT * FROM noncompanycustomers"

    sqlitefragment_cursor.execute(sqlitefragment_query)
    sqlitefragment_queryresults = sqlitefragment_cursor.fetchall()
    print(sqlitefragment_queryresults)


    connection_cursor.execute(connection_cursor_query)
    connection_cursor_queryresults = connection_cursor.fetchall()
    print(connection_cursor_queryresults)


    mysqlFragment_cursor.execute("""
            CREATE TABLE IF NOT EXISTS `ReconstructedCustomersFromCompany` (
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

    mysqlFragment_clear = "DELETE FROM ReconstructedCustomersFromCompany"
    mysqlFragment_cursor.execute(mysqlFragment_clear)

    mysqlFragment_insert_customers_sql = """
        INSERT INTO `ReconstructedCustomersFromCompany`
        (`CustomerId`,`FirstName`,`LastName`,`Company`,`Address`,`City`,`State`,`Country`,`PostalCode`,`Phone`,`Fax`,`Email`,`SupportRepId`)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """
    mysqlFragment_cursor.executemany(mysqlFragment_insert_customers_sql, sqlitefragment_queryresults)
    mysqlFragment_cursor.executemany(mysqlFragment_insert_customers_sql, connection_cursor_queryresults)
    mysqlFragment.commit()




reconstruct_primary_horizontal_fragment_1()
reconstruct_primary_horizontal_fragment_2()

