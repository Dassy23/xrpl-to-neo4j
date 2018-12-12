# Tools for synchronizing the XRP ledger with a Neo4j database

This code allows you to fetch all transactions from the XRP ledger and insert them into a Neo4j database. 

## Setup

If you want to insert the data in your own [Neo4j](https://neo4j.com) database, make sure to update the credentials and database location in shared.js:

```
const driver = neo4j.driver("bolt://localhost", neo4j.auth.basic("neo4j", "1234"));
```

## Database population

You can populate the database in two ways, using SQLite files or live from a rippled node. No matter which you use, it will set up the project and continue from the first available ledger in the source.

### Note

This means that if you populate from a current non-complete SQLite file, running index.js will pick up from the latest ledger index in the Neo4j database, and you will not include all history. 

Wallets will also only be created from the transactions where they were initially created, so any transactions involving a wallet that is not in the Neo4j database *will not be included*.

### Initial population from SQLite files

If you have access to a *full history* transaction.db and ledger.db from a rippled node, you can add the files to the project and run `node sql.js`.

## Population from a ripple host

Start the script using `node index.js`. Change the rippled node address if you want to use an alternative node.

This can also be used to keep the database in sync after populating with SQLite files.