import sqlite3
import random
import time

"""
Benchmarking of LIKE queries
"""

DATABASE = "index_like_query_perf.db"
ROW_COUNT = 3_000_000
QUERY_COUNT = 100
MAX_SUBSIDIARY_ID = 100_000

def generateSubsidiaryId():
    return random.randint(1, MAX_SUBSIDIARY_ID)

def generateBirthDate():
    return f"{random.randint(1975, 1980)}-{random.randint(1, 12)}-{random.randint(1, 28)}"

def generateRandomName(length = 10):
    return ''.join(random.choices("abcdefghijklm", k=length))

def createTable():
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS employees")
    cur.execute("""
                CREATE TABLE 
                    employees (employee_id BIGINT, name TEXT, subsidiary_id BIGINT, birth_date DATE)
                """
                )
    cur.execute("CREATE INDEX idx_employee_id_subsidiary_id ON employees(employee_id, subsidiary_id)")
    cur.execute("CREATE INDEX idx_employeee_name_ ON employees(name COLLATE NOCASE)")
    con.commit()
    con.close()  

def averageQueryTime(tableName, name):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    total = 0
    for x in range(QUERY_COUNT):
        start = time.perf_counter_ns()
        cur.execute(f"SELECT * FROM {tableName} WHERE name LIKE '{name}'")
        elapsed = time.perf_counter_ns() - start
        total += elapsed
    con.close()
    return total / QUERY_COUNT  

def fillTable(tableName, rows):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    for x in range(rows):
        name = generateRandomName()
        cur.execute(f"INSERT INTO {tableName} VALUES (?, ?, ?, ?)", (x, name, generateSubsidiaryId(), generateBirthDate()))
    con.commit()
    con.close()    

def main():
    createTable()
    fillTable("employees", ROW_COUNT)

    averageTimeMoreFiltering = averageQueryTime("employees", "abc%lm")
    averageTimeMoreLessFiltering = averageQueryTime("employees", "a%jklm")
    averageTimeEndsWith = averageQueryTime("employees", "%ijklm")

    
    # this should be fastest, more rows are filtered out by index and 
    # less left for scanning to find out names ending with 'ij'
    print(f"Average time to find abc%lm:    {averageTimeMoreFiltering}")

    # this should be slower than previous one, because less rows are filtered 
    # out because of broader 'abc%' search criterie, rest should be found 
    # by scanning
    print(f"Average time to find a%jklm:    {averageTimeMoreLessFiltering}")

    # this should be the slowest, because index cannot be used in this case,
    # so all rows must be scanned to find out names ending with 'ij'
    print(f"Average time to find %ijklm:    {averageTimeEndsWith}")

if __name__ == "__main__":
    main()