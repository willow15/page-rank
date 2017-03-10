# description: simple version of page rank

import sqlite3

conn = sqlite3.connect("repository.sqlite")
cur = conn.cursor()

# 0. store data in memory
from_ids = list()
cur.execute('SELECT DISTINCT from_id FROM Links')
for row in cur:
    from_ids.append(row[0])

to_ids = list()
cur.execute('SELECT DISTINCT to_id FROM Links')
for row in cur:
    to_ids.append(row[0])

links = list()
cur.execute('SELECT from_id, to_id FROM Links')
for row in cur:
    if row[0] == row[1]: continue
    if row[0] not in from_ids: continue
    links.append(row)

prev_ranks = dict()
cur.execute('SELECT id, new_rank FROM Pages')
for row in cur:
    prev_ranks[row[0]] = row[1]

total_nodes = len(prev_ranks)

# 1. decide how many iterations
sval = raw_input("How many iterations: ")
if len(sval) > 0: iteration = int(sval)
else: iteration = 1
for i in range(iteration):

    # 2. compute new rank
    next_ranks = dict()
    total_diff = 0.0
    for from_node in from_ids:

        # 2.1. compute the number of links going out of a page
        give_ids = list()
        for (from_id, to_id) in links:
            if from_node != from_id: continue
            give_ids.append(to_id)

        # 2.2. set new rank to to_id
        for to_node in give_ids:
            next_ranks[to_node] = next_ranks.get(to_node, 0.0) + prev_ranks[from_node] / len(give_ids)
        total_diff = total_diff + prev_ranks[from_node]

    # 3. compute the per-page change from old rank to new rank as indication of convergence of the algorithm
    ave_diff = total_diff / total_nodes
    print "iteration:", i, "average difference:", ave_diff

    # 4. write back to database
    cur.execute('UPDATE Pages SET old_rank = new_rank')
    for (node, rank) in next_ranks.items():
        prev_ranks[node] = rank
        cur.execute('UPDATE Pages SET new_rank = ? WHERE id = ?', (rank, node))
    conn.commit()

cur.close()
