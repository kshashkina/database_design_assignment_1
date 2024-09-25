import time

import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Connection settings
HOST = 'localhost'
USER = 'root'
PASSWORD = '-Nt+ab&AkDL5idx'
DATABASE = 'practices'


def create_connection():
    try:
        # Establish a connection to the MySQL database
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )
        # Check if the connection was successful
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: {e}")
    return None


def read_uncommited_demo():
    """
    Demonstrates the READ UNCOMMITTED isolation level.
    Shows how a dirty read occurs.
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Start transaction with READ UNCOMMITTED isolation level
        print(f"[{datetime.now()}] Transaction 1 started.")
        connection1.start_transaction(isolation_level='READ UNCOMMITTED')

        # Update Alice's balance in Transaction 1
        cursor1.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")
        print(f"[{datetime.now()}] Transaction 1: Updated Alice's balance to 9999 (not committed yet).")

        # Transaction 2: Start another transaction with READ UNCOMMITTED isolation level
        print(f"[{datetime.now()}] Transaction 2 started.")
        connection2.start_transaction(isolation_level='READ UNCOMMITTED')

        # Read Alice's balance in Transaction 2
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_dirty_read = cursor2.fetchone()[0]

        # Print the dirty read result
        print(f"[{datetime.now()}] Transaction 2: Dirty Read (READ UNCOMMITTED): Alice's balance = {balance_dirty_read}")

        # Rollback Transaction 1, reverting the balance change
        print(f"[{datetime.now()}] Transaction 1: Rolling back the transaction.")
        connection1.rollback()

        # Commit Transaction 2
        print(f"[{datetime.now()}] Transaction 2: Committing the transaction.")
        connection2.commit()

        # Print the final balance for verification
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        final_balance = cursor2.fetchone()[0]
        print(f"[{datetime.now()}] Transaction 2: Final balance after rollback (should be original): Alice's balance = {final_balance}")

    except Error as e:
        print(f"Error: {e}")
    finally:
        # Clean up and close cursors and connections
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()


def read_committed_demo():
    """
    Demonstrates the READ COMMITTED isolation level.
    Shows how a consistent read occurs.
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Start transaction with READ COMMITTED isolation level
        print(f"[{datetime.now()}] Transaction 1 started.")
        connection1.start_transaction(isolation_level='READ COMMITTED')

        # Update Alice's balance in Transaction 1
        cursor1.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")
        print(f"[{datetime.now()}] Transaction 1: Updated Alice's balance to 9999 (not committed yet).")

        # Transaction 2: Start another transaction with READ COMMITTED isolation level
        print(f"[{datetime.now()}] Transaction 2 started.")
        connection2.start_transaction(isolation_level='READ COMMITTED')

        # Read Alice's balance in Transaction 2
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_read_committed = cursor2.fetchone()[0]

        # Print the read result
        print(f"[{datetime.now()}] Transaction 2: Read (READ COMMITTED): Alice's balance = {balance_read_committed}")

        # Commit Transaction 1
        print(f"[{datetime.now()}] Transaction 1: Committing the transaction.")
        connection1.commit()

        # Transaction 2: Read Alice's balance again after committing Transaction 1
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        final_balance = cursor2.fetchone()[0]
        print(f"[{datetime.now()}] Transaction 2: Final balance after commit (should reflect updated balance): Alice's balance = {final_balance}")

    except Error as e:
        print(f"Error: {e}")
    finally:
        # Clean up and close cursors and connections
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()


def repeatable_read_demo():
    """
    Demonstrates the REPEATABLE READ isolation level.
    Shows how a consistent repeatable read occurs during a transaction.
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Start transaction with REPEATABLE READ isolation level
        print(f"[{datetime.now()}] Transaction 1 started.")
        connection1.start_transaction(isolation_level='REPEATABLE READ')

        # Read Alice's balance in Transaction 1
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_initial_read = cursor1.fetchone()[0]
        print(f"[{datetime.now()}] Transaction 1: Initial Read: Alice's balance = {balance_initial_read}")

        # Transaction 2: Start another transaction and update Alice's balance
        print(f"[{datetime.now()}] Transaction 2 started.")
        connection2.start_transaction(isolation_level='READ COMMITTED')
        cursor2.execute("UPDATE accounts SET balance = 5000 WHERE name = 'Alice'")
        print(f"[{datetime.now()}] Transaction 2: Updated Alice's balance to 5000.")
        connection2.commit()  # Commit Transaction 2

        # Transaction 1: Read Alice's balance again in Transaction 1 (should show the same value)
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_second_read = cursor1.fetchone()[0]
        print(f"[{datetime.now()}] Transaction 1: Second Read: Alice's balance = {balance_second_read}")

        # Commit Transaction 1
        connection1.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        # Clean up and close cursors and connections
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()

def non_repeatable_read_demo():
    """
    Demonstrates the NON REPEATABLE READ isolation level.
    Shows how a value read can change between two reads due to another transaction.
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Start transaction with READ COMMITTED isolation level
        print(f"[{datetime.now()}] Transaction 1 started.")
        connection1.start_transaction(isolation_level='READ COMMITTED')

        # Read Alice's balance in Transaction 1
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_first_read = cursor1.fetchone()[0]
        print(f"[{datetime.now()}] Transaction 1: First Read: Alice's balance = {balance_first_read}")

        # Transaction 2: Start another transaction and update Alice's balance
        print(f"[{datetime.now()}] Transaction 2 started.")
        connection2.start_transaction(isolation_level='READ COMMITTED')
        cursor2.execute("UPDATE accounts SET balance = 5000 WHERE name = 'Alice'")
        print(f"[{datetime.now()}] Transaction 2: Updated Alice's balance to 5000.")
        connection2.commit()  # Commit Transaction 2

        # Read Alice's balance again in Transaction 1 (should show the new value)
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_second_read = cursor1.fetchone()[0]
        print(f"[{datetime.now()}] Transaction 1: Second Read: Alice's balance = {balance_second_read}")

        # Commit Transaction 1
        connection1.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        # Clean up and close cursors and connections
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()

def deadlock_demo():
    """
    Demonstrates a deadlock situation between two transactions
    trying to lock Alice's account.
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Start transaction and lock Alice
        print(f"[{datetime.now()}] Transaction 1 started.")
        connection1.start_transaction(isolation_level='READ COMMITTED')
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice' FOR UPDATE")
        print(f"[{datetime.now()}] Transaction 1: Locked Alice's account.")

        # Simulate processing time
        time.sleep(1)

        # Transaction 2: Start transaction and try to lock Alice's account
        print(f"[{datetime.now()}] Transaction 2 started.")
        connection2.start_transaction(isolation_level='READ COMMITTED')
        # Deadlock will occur here
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice' FOR UPDATE")
        print(f"[{datetime.now()}] Transaction 2: Attempting to lock Alice's account.")

        # This point will not be reached due to deadlock
        connection1.commit()
        connection2.commit()

    except Error as e:
        print(f"Deadlock Error: {e}")
        # Rollback transactions due to deadlock
        if connection1.is_connected():
            connection1.rollback()
            print(f"[{datetime.now()}] Transaction 1 rolled back due to deadlock.")
        if connection2.is_connected():
            connection2.rollback()
            print(f"[{datetime.now()}] Transaction 2 rolled back due to deadlock.")
    finally:
        # Clean up and close cursors and connections
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()

if __name__ == "__main__":
    #print("Read Uncommitted Transactions")
    #read_uncommited_demo()
    #print("\nRead Committed Transactions")
    #read_committed_demo()
    #print("\nRepeatable Read Transactions")
    #repeatable_read_demo()
    #print("\nNon Repeatable Read Transactions")
    #non_repeatable_read_demo()
    print("\nDeadlock Demonstration")
    deadlock_demo()
