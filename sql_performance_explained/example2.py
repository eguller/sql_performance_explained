import sqlite3
import random
import time


"""
Benchmarking the performance of a query with different order of columns in the index
In this example since we query only with subsidiary_id (employee_id, subsidiary_id) index cannot be used.
But (subsidiary_id, employee_id) index can be used.
"""

DATABASE = "example2.db"
ROW_COUNT = 1_000_000
QUERY_COUNT = 1_000
MAX_SUBSIDIARY_ID = 100_000

def generateSubsidiaryId():
    return random.randint(1, MAX_SUBSIDIARY_ID)

def generateBirthDate():
    return f"{random.randint(1975, 1980)}-{random.randint(1, 12)}-{random.randint(1, 28)}"

def createWithIndexEmployeeIdFirst():
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS employees_with_employee_id_first_index")
    cur.execute("""
                CREATE TABLE 
                    employees_with_employee_id_first_index (employee_id BIGINT, subsidiary_id BIGINT, birth_date DATE)
                """
                )
    cur.execute("CREATE INDEX idx_employee_id_subsidiary_id ON employees_with_employee_id_first_index(employee_id, subsidiary_id)")
    con.commit()
    con.close()  

def createWithIndexSubsidiaryIdFirst():
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS employees_with_subsidiary_id_first_index")
    cur.execute("""
                CREATE TABLE 
                    employees_with_subsidiary_id_first_index (employee_id BIGINT, subsidiary_id BIGINT, birth_date DATE)
                """
                )
    cur.execute("CREATE INDEX idx_subsidiary_id_employee_id ON employees_with_subsidiary_id_first_index(subsidiary_id, employee_id)")
    con.commit()
    con.close()    

def averageQueryTime(tableName):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    total = 0
    for x in range(QUERY_COUNT):
        subsidiaryId = generateSubsidiaryId()
        start = time.perf_counter_ns()
        cur.execute(f"SELECT * FROM {tableName} WHERE subsidiary_id = ?", (subsidiaryId,))
        elapsed = time.perf_counter_ns() - start
        total += elapsed
    con.close()
    return total / QUERY_COUNT  

def fillTable(tableName, rows):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    for x in range(rows):
        cur.execute(f"INSERT INTO {tableName} VALUES (?, ?, ?)", (x, generateSubsidiaryId(), generateBirthDate()))
    con.commit()
    con.close()    

def main():
    createWithIndexEmployeeIdFirst()
    fillTable("employees_with_employee_id_first_index", ROW_COUNT)

    createWithIndexSubsidiaryIdFirst()
    fillTable("employees_with_subsidiary_id_first_index", ROW_COUNT)

    # second query should perform better than first one in average.
    # In first query database uses table scan to find matching subsidiary id
    # In second query index is used. 
    # Query performance gets larger when subsidiary id range gets larger, because 
    # more rows have to be scanned to find a matching one.
    averageTimeEmployeesEmployeeIdFirstIndex = averageQueryTime("employees_with_employee_id_first_index")
    print(f"Average time employee id first index: {averageTimeEmployeesEmployeeIdFirstIndex}")

    averageTimeEmployeesSubsidiaryIdFirstIndex = averageQueryTime("employees_with_subsidiary_id_first_index")
    print(f"Average time subsidiary id first index: {averageTimeEmployeesSubsidiaryIdFirstIndex}")

if __name__ == "__main__":
    main()