\set naccounts 100000 * :scale
\set updateaid random(7500000,12000000)
\set selectaid random(75000000,:naccounts)
\set unionaid random(60000000,:naccounts)
\set delta random(-5000,5000)
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid <= :updateaid;
SELECT * FROM pgbench_accounts WHERE aid <= :selectaid;
SELECT aid, abalance from pgbench_accounts where aid <= :unionaid UNION SELECT bid, bbalance from pgbench_branches where bid <= :bid;
END;