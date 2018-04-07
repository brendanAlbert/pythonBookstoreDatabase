"""
    Brendan Albert
    Assignment 13 - Databases One-To-Many, Many-To-Many
"""
import sqlite3 as sq
from collections import defaultdict


def connect():
    """
    Establishes connection and pointer to database.
    :return: returns a tuple containing the connection and cursor
    """
    conn = sq.connect('bookstore.sqlite')
    curr = conn.cursor()
    return conn, curr


def clean_database(curr, tables):
    """
    Wipes out any existing tables in the database.
    """
    for table in tables:
        cmd = "DROP TABLE IF EXISTS {}".format(table)
        curr.execute(cmd)


def create_book_table(curr):
    """
    Creates a table titled `Book` in the bookstore database.
    """
    cmd = """
    CREATE TABLE IF NOT EXISTS Book(
    barcode INTEGER PRIMARY KEY NOT NULL,
    title TEXT,
    author TEXT
    );
    """
    curr.execute(cmd)


def create_patron_table(curr):
    """
    Creates a table titled `Patron` in the bookstore database.
    """
    cmd = """
    CREATE TABLE IF NOT EXISTS Patron(
    card_number INTEGER PRIMARY KEY NOT NULL,
    name TEXT,
    zipcode INTEGER
    );
    """
    curr.execute(cmd)


def create_loan_table(curr):
    """
    Creates a table titled `Loan` in the bookstore database.
    """
    cmd = """
    CREATE TABLE IF NOT EXISTS Loan(
        card_id INTEGER NOT NULL,
        book_id INTEGER NOT NULL UNIQUE,
        due_date TEXT NOT NULL,
        PRIMARY KEY (card_id, book_id)
    );"""
    curr.execute(cmd)


def populate_tables(conn, curr):
    """
    Adds records to each table.
    """

    cmd = """
    INSERT INTO Book(barcode, title, author)
    VALUES (12345, 'Dune', 'Frank Herbert'),
    (15, '1984', 'George Orwell'),
    (16, 'Animal Farm', 'George Orwell'),
    (17, 'Fahrenheit 451', 'Ray Bradbury'),
    (18, "Ender's Game", 'Orson Scott Card'),
    (6789, 'The Hobbit', 'J.R.R. Tolkein'),
    (1984, "Harry Potter and the Sorceror's Stone", 'J.K. Rowling'),
    (123, 'Green Eggs and Ham', 'Dr. Seuss'),
    (101101, 'The Giving Tree', 'Shel Silverstein');
    """
    curr.execute(cmd)
    conn.commit()

    cmd = """
    INSERT INTO Patron(card_number, name, zipcode)
    VALUES (101, 'Ben', 12345),
    (102, 'Ralph', 23456),
    (103, 'Gwen', 34567),
    (104, 'Tim', 45678),
    (105, 'Charlize', 56789);
    """
    curr.execute(cmd)
    conn.commit()

    cmd = """
    INSERT INTO Loan(card_id, book_id, due_date)
    VALUES (101, 12345, '4/10/18'),
    (102, 6789, '4/12/18'),
    (103, 1984, '4/14/18'),
    (104, 123, '4/16/18'),
    (105, 101101, '4/18/18'),
    (101 , 15 , '4/10/18'),
    (101 , 16 , '4/10/18'),
    (101 , 17 , '4/10/18'),
    (102 , 18 , '4/6/18')
    """
    curr.execute(cmd)
    conn.commit()


def pretty_print_book_table(curr):
    """
    Gets the data from the book table via an SQL query.
    Prints the fields and records in a neatly formatted way.
    """

    cmd = 'SELECT * FROM Book'
    curr.execute(cmd)
    records = curr.fetchall()

    print('Book Table')
    print("{:12s}{:40s}{:30s}".format('Barcode', 'Title', 'Author'))
    print('.'*70)
    for rec in records:
        print("{:<12d}{:40s}{:30s}".format(rec[0], rec[1], rec[2]))
    print()


def pretty_print_patron_table(curr):
    """
    Gets the data from the patron table via an SQL query.
    Prints the fields and records in a neatly formatted way.
    """

    cmd = 'SELECT * FROM Patron'
    curr.execute(cmd)
    records = curr.fetchall()

    print('Patron Table')
    print("{:18s}{:20s}{:20s}".format('Card Number', 'Name', 'Zipcode'))
    print('.'*50)
    for rec in records:
        print("{:<18d}{:20s}{:5d}".format(rec[0], rec[1], rec[2]))
    print()


def pretty_print_loan_table(curr):
    """
    Gets the data from the Loan table via an SQL query.
    Prints the fields and records in a neatly formatted way.
    """

    cmd = 'SELECT * FROM Loan'
    curr.execute(cmd)
    records = curr.fetchall()

    print('Loan Table')
    print("{:18s}{:20s}{:20s}".format('Card ID', 'Book ID', 'Duedate'))
    print('.'*50)
    for rec in records:
        print("{:<18d}{:<20d}{:20s}".format(rec[0], rec[1], rec[2]))
    print()


def print_zip_code(curr):
    """
    Select only the zipcode entries from the patron table via an SQL query.
    Prints the field and records in a neatly formatted way.
    """

    cmd = 'SELECT zipcode FROM Patron'
    curr.execute(cmd)
    records = curr.fetchall()
    print()
    print("{:18s}".format('Zipcodes'))
    print('.'*10)
    for rec in records:
        print("{:<18d}".format(rec[0]))
    print()


def print_book_table_author_desc(curr):
    """
    Select all the book records from the book table via an SQL query.
    Prints the field and records in a neatly formatted way.
    The authors are ordered in descending order.
    """

    cmd = """
    SELECT * FROM Book
    ORDER BY
    author DESC
    """
    curr.execute(cmd)
    records = curr.fetchall()

    print('\nAuthors sorted in descending order')
    print("{:12s}{:40s}{:30s}".format('Barcode', 'Title', 'Author'))
    print('.'*70)
    for rec in records:
        print("{:<12d}{:40s}{:30s}".format(rec[0], rec[1], rec[2]))
    print()


def print_book_title_author(curr):
    """
    Select only the title and author fields from the book table via an SQL query.
    Prints the field and records in a neatly formatted way.
    """

    cmd = """
    SELECT title, author FROM Book
    """
    curr.execute(cmd)
    records = curr.fetchall()

    print("{:40s}{:30s}".format('Title', 'Author'))
    print('.'*50)
    for rec in records:
        print("{:40s}{:30s}".format(rec[0], rec[1]))
    print()


def delete_third_book(conn, curr):
    """
    Deletes the third book from the book table.
    If the books had a primary key, that would have been used.
    """

    cmd = """
    DELETE FROM Book
    WHERE barcode=1984
    """
    curr.execute(cmd)
    conn.commit()
    print('\nNotice that Harry Potter is now gone')
    print_book_title_author(curr)


def insert_new_book(conn, curr):
    """
    Inserts a new book into the books table via an SQL statement/query.
    """

    cmd = """
    INSERT INTO Book(barcode, title, author)
    VALUES(?, ?, ?)
    """
    curr.execute(cmd, (3331642, 'The Giver Blue', 'Lois Lowry'))
    conn.commit()
    print('\nNotice that The Giver Blue has been added')
    print_book_title_author(curr)


def update_book(conn, curr):
    """
    Updates a book record in the book table using an SQL query/statement.
    """
    cmd = """
    UPDATE Book
    SET title='Gathering Blue'
    WHERE title='The Giver Blue'
    """
    curr.execute(cmd)
    conn.commit()
    print('\nThe Giver Blue has been updated to Gathering Blue')
    print_book_title_author(curr)


def print_patrons_with_books_checked_out(curr):
    """
    Gets the data from the patron table and the books each patron
    checked out, via an SQL query.
    Prints the fields and records in a neatly formatted way.
    """

    # <   patron           >    <       loan        >   <      book      >
    # patron <-> card_number <-> card_id <-> book_id <-> barcode <-> title
    curr.execute('SELECT * FROM Patron')
    records = curr.fetchall()

    print('Patron Table with Books Checked Out')
    print("{:18s}{:20s}{:20s}".format('Card Number', 'Name', 'Zipcode'))
    print('.'*50)
    for rec in records:
        card_number, name, zipcode = rec[0], rec[1], rec[2]
        print("{:<18d}{:20s}{:5d}".format(card_number, name, zipcode))
        curr.execute("SELECT book_id FROM Loan WHERE card_id = ?", (card_number,))
        print(name, 'has checked out:')
        book_ids = curr.fetchall()

        for id_tuple in book_ids:
            curr.execute('SELECT title FROM Book WHERE barcode = ?', id_tuple)
            print('  -', curr.fetchone()[0])

    print()

def print_table_currently_checked_out_books(curr):
    """
    Prints out a table of the books that are currently checked out.
    The table includes the title, author and due date.
    :return:
    """

    curr.execute('SELECT * FROM Loan')
    checked_out = curr.fetchall()  # [(101, 12345, '4/10/18')]

    print("\nBooks currently checked out: ")
    print("{:<42s}{:25s}{:20s}".format("Title", "Author", "Due Date"))
    print('.' * 80)
    for book in checked_out:

        curr.execute('SELECT title, author FROM Book WHERE barcode = ?', (book[1],))
        records = curr.fetchall()
        for rec in records:
            print("{:<42s}{:25s}{:20s}".format(rec[0], rec[1], book[2]))

    print()


def print_table_patrons_who_checked_out_books(curr):
    """
    Prints out a table of people who actually checked out books.
    The table contains the person's name, card number and book title.
    :param curr:
    :return:
    """
    curr.execute('SELECT card_id, book_id FROM Loan')
    checked_out = curr.fetchall()  # [(101, 12345)]

    patron_checked_books_dict = {}
    for loan_tuple in checked_out:
        patron_checked_books_dict.setdefault(loan_tuple[0], []).append(loan_tuple[1])

    #print(patron_checked_books_dict)

    print("\nPatrons who have checked out books, and those books' titles.")
    print("{:<22s}{:25s}{}".format("Name", "Card Number", "Book Title"))
    print('.'*80)
    for id in patron_checked_books_dict:
        #print(id)
        curr.execute('SELECT name FROM Patron WHERE card_number = ?', (id,))
        print("{:<25}{}".format(curr.fetchone()[0], id))

        for book_id in patron_checked_books_dict[id]:
            #print(book_id)
            curr.execute('SELECT title FROM Book WHERE barcode = ?', (book_id,))
            ids = curr.fetchall()
            print("{:<47s}-{}".format('', ids[0][0]))
        print('-'*80)


        #
        # print(names[0], book[0])
        #



def main():
    """
    Entry point of the program.
    """
    tables = ["Book", "Loan", "Patron"]
    # book db
    conn, curr = connect()
    clean_database(curr, tables)
    #
    # #create tables
    create_book_table(curr)
    create_patron_table(curr)
    create_loan_table(curr)
    #
    populate_tables(conn, curr)
    #
    # pretty_print_book_table(curr)
    # pretty_print_patron_table(curr)
    pretty_print_loan_table(curr)

    print_patrons_with_books_checked_out(curr)
    print_table_currently_checked_out_books(curr)
    print_table_patrons_who_checked_out_books(curr)

    #print_zip_code(curr)
    #print_book_table_author_desc(curr)
    #print_book_title_author(curr)
    #delete_third_book(conn, curr)
    #insert_new_book(conn, curr)
    #update_book(conn, curr)

    # close db connection
    conn.close()


if __name__ == "__main__":
    main()
