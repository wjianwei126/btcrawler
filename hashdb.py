
import sqlite3

class HashDB:
    def __init__(self, filename='hash.db'):
        self.conn = sqlite3.connect(filename)
        self.cursor = self.conn.cursor()
        try:
            self.cursor.execute('''CREATE TABLE hashes (hash varchar(40) PRIMARY KEY)''')
        except sqlite3.OperationalError, e:
            if str(e) != 'table hashes already exists':
                raise e

    def release(self):
        self.conn.close()

    def insert_hash(self, h):
        try:
            self.cursor.execute('''INSERT INTO hashes VALUES ('%s')''' % (h))
            self.conn.commit()
        except sqlite3.IntegrityError, e:
            pass

    def fetch_all_hashes(self):
        try:
            result = self.cursor.execute('''SELECT * FROM hashes''')
            hashlist = []
            for row in result:
                hashlist.append(str(row[0]))
            return hashlist
        except Exception, e:
            return None

    def exist(self, h):
        try:
            result = self.cursor.execute('''SELECT * FROM hashes WHERE hash='%s' ''' % (h))
            return (len(result.fetchall()) > 0)
        except Exception, e:
            raise e

def main():
    db = HashDB()
    print db.fetch_all_hashes()

if __name__ == '__main__':
    main()