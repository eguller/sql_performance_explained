
import sqlite3
import random
import time
import string

"""
Benchmarking the performance of a message queue with full and partial index
"""
DATABASE = "partial_index.db"
ROW_COUNT = 2_000_000
QUERY_COUNT = 100

def generateMessage():
    return ''.join(random.choices(string.ascii_lowercase, k=16))

def generateReceiver():
    return ''.join(random.choices("abcdef", k=4))

def createTableWithFullIndex():
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS messages_full_index")
    cur.execute("""
                CREATE TABLE 
                    messages_full_index (message_id BIGINT, message TEXT, receiver TEXT, processed CHARACTER(1))
                """
                )
    cur.execute("CREATE INDEX idx_messages_full_index ON messages_full_index(receiver, processed)")
    con.commit()
    con.close()

def createTableWithPartialIndex():
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS messages_with_partial_index")
    cur.execute("""
                CREATE TABLE 
                    messages_with_partial_index (message_id BIGINT, message TEXT, receiver TEXT, processed CHARACTER(1))
                """
                )
    cur.execute("CREATE INDEX idx_messages_partial_index ON messages_with_partial_index(receiver) WHERE processed = 'N'")
    con.commit()
    con.close()    

def fillTable(tableName, rows):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    
    for x in range(rows):
        processed = "N" if random.randint(0, ROW_COUNT) < 1000 else "Y"
        cur.execute(f"INSERT INTO {tableName} VALUES (?, ?, ?, ?)", (x, generateMessage(), generateReceiver(), processed))   
    
    con.commit()
    con.close() 

def averageQueryTime(tableName):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    total = 0
    for _ in range(QUERY_COUNT):
        start = time.perf_counter_ns()
        cur.execute(f"SELECT * FROM {tableName} WHERE receiver = ? AND processed = 'N'", (generateReceiver(),))
        elapsed = time.perf_counter_ns() - start
        total += elapsed
    con.close()
    return total / QUERY_COUNT       

def main():
    createTableWithFullIndex()
    fillTable("messages_full_index", ROW_COUNT)

    createTableWithPartialIndex()
    fillTable("messages_with_partial_index", ROW_COUNT)

    # This example is obvious query finds matching rows faster when index is used.
    # Without index database uses table scan to find matching rows which is much slower than 
    # index in this case.
    averateTimeWithPartialIndex = averageQueryTime("messages_with_partial_index")
    print(f"Average time messages_with_partial_index: {averateTimeWithPartialIndex}")

    averageTimeWithFullIndex = averageQueryTime("messages_full_index")
    print(f"Average time messages_full_index:         {averageTimeWithFullIndex}")

if __name__ == "__main__":
    main()    