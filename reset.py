# description: reset rank

import sqlite3

conn = sqlite3.connect("repository.sqlite")
cur = conn.cursor()
cur.execute('UPDATE Pages SET old_rank = 0.0, new_rank = 1.0')
conn.commit()
cur.close()
