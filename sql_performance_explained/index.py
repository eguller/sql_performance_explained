import sqlite3
import random
import time
"""
Benchmarking the performance of a query with and without an index
"""
DATABASE = "index.db"
ROW_COUNT = 1_000_000
QUERY_COUNT = 1_000

def generateSubsidiaryId():
    return random.randint(1, 100)

def generateBirthDate():
    return f"{random.randint(1975, 1980)}-{random.randint(1, 12)}-{random.randint(1, 28)}"

def fillTable(tableName, rows):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    for x in range(rows):
        cur.execute(f"INSERT INTO {tableName} VALUES (?, ?, ?)", (x, generateSubsidiaryId(), generateBirthDate()))
    con.commit()
    con.close()

def createWithoutIndex():
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS employees_without_index")
    cur.execute("""
                CREATE TABLE 
                    employees_without_index (employee_id BIGINT, subsidiary_id BIGINT, birth_date DATE)
                """
                )
    con.commit()
    con.close()

def createWithIndex():
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS employees_with_index")
    cur.execute("""
                CREATE TABLE 
                    employees_with_index (employee_id BIGINT, subsidiary_id BIGINT, birth_date DATE)
                """
                )
    cur.execute("CREATE INDEX idx_employee_id_subsidiary_id ON employees_with_index(employee_id, subsidiary_id)")
    con.commit()
    con.close()    

def averageQueryTime(tableName):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    total = 0
    for _ in range(QUERY_COUNT):
        subsidiaryId = random.randint(1, 100)
        employeeId = random.randint(1, ROW_COUNT)
        start = time.perf_counter_ns()
        cur.execute(f"SELECT * FROM {tableName} WHERE employee_id = ? AND subsidiary_id = ?", (employeeId, subsidiaryId))
        elapsed = time.perf_counter_ns() - start
        total += elapsed
    con.close()
    return total / QUERY_COUNT

def main():
    createWithoutIndex()
    fillTable("employees_without_index", ROW_COUNT)

    createWithIndex()
    fillTable("employees_with_index", ROW_COUNT)

    # This example is obvious query finds matching rows faster when index is used.
    # Without index database uses table scan to find matching rows which is much slower than 
    # index in this case.
    averageTimeEmployeesWithoutIndex = averageQueryTime("employees_without_index")
    print(f"Average time employees_without_index: {averageTimeEmployeesWithoutIndex}")

    averageTimeEmployeesWithIndex = averageQueryTime("employees_with_index")
    print(f"Average time employees_with_index: {averageTimeEmployeesWithIndex}")

if __name__ == "__main__":
    main()