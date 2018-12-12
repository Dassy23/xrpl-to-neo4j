const binarycodec = require('ripple-binary-codec');
const sqlite = require('sqlite');
const shared = require('./shared.js');

const ledgersPerQuery = 1000;
const commitSize = 10000;

const ledgerDbPromise = sqlite.open('./ledger.db', { Promise });
const transactionDbPromise = sqlite.open('./transaction.db', { Promise });

// Open databases
console.log("Opening SQLite databases");
Promise.all([ledgerDbPromise, transactionDbPromise]).then(dbOpenResults => {
	let Stopped = false

	const ledgerDB = dbOpenResults[0];
	const transactionDB = dbOpenResults[1];
	
	var activations = [];
	var account_changes = [];
	var payments = [];
	
	const fetchTransactions = (ledgerIndex) => {
		return new Promise((resolve, reject) => {
			// Fetching next group of ledgers
			return ledgerDB.all('SELECT LedgerSeq, ClosingTime FROM Ledgers WHERE LedgerSeq >= ? ORDER BY LedgerSeq ASC LIMIT ?', ledgerIndex, ledgersPerQuery).then(ledgers => {
				
				// Check if we're done.
				if(ledgers.length === 0) {
					resolve({activations: [], account_changes: [], payments: [], lastLedgerIndex: 0, done: true});
					return;
				}
				
				var ledgerMap = {};
				var ledgersToQuery = [];
				ledgers.forEach(ledger => {
					ledgerMap[ledger.LedgerSeq] = new Date((ledger.ClosingTime + 946684800) * 1000).toISOString();
					ledgersToQuery.push(ledger.LedgerSeq);
				});
								
				// Fetching all transactions
				return transactionDB.all(`SELECT TransID, LedgerSeq, hex(RawTxn) AS RawTxn, hex(TxnMeta) AS TxnMeta FROM Transactions WHERE (TransType = "AccountSet" OR TransType = "Payment") AND LedgerSeq IN (${ledgersToQuery.join(',')}) ORDER BY LedgerSeq ASC, rowid ASC`).then(transactions => {

					let Txs = transactions.map(Tx => {
						return Object.assign(binarycodec.decode(Tx.RawTxn), {
							metaData: binarycodec.decode(Tx.TxnMeta),
							ClosingTime: ledgerMap[Tx.LedgerSeq],
							LedgerSeq: Tx.LedgerSeq,
							hash: Tx.TransID
						})
					})
					
					return prepareLedgerTransactions(Txs).then(result => {
						resolve({activations: result.activations, account_changes: result.account_changes, payments: result.payments, lastLedgerIndex: ledgersToQuery[ledgersToQuery.length - 1], done: false});
					})
				}).catch(reject);
			}).catch(reject);
		})
	}
	
	const run = (ledgerIndex) => {	
		fetchTransactions(ledgerIndex).then(result => {
			
			activations.push(...result.activations);
			account_changes.push(...result.account_changes);
			payments.push(...result.payments);
			
			if (Stopped) {
				return
			}
			
			if(result.done) {
				if(activations.length + payments.length + account_changes.length > 0) {
					console.log(`Graph database commit: Total count ${activations.length + payments.length + account_changes.length}`)
					
					commitToServer(activations, account_changes, payments).then(commitResult => {
						console.log("Graph database commit: Done\nBackfiller is done");
						endScript();
					}).catch(error => {
						console.log("Graph database commit: Error: " + error);
						endScript();
						process.exit(1);
					});
				}
				else {
					console.log("Backfiller is done");
					endScript();
				}
			}
			else if(activations.length + payments.length + account_changes.length >= commitSize) {
				console.log(`Graph database commit: Total count ${activations.length + payments.length + account_changes.length} – next ledger ${result.lastLedgerIndex + 1}`)

				commitToServer(activations, account_changes, payments).then(commitResult => {
					console.log("Graph database commit: Done");
					
					// Empty collectors after commit
					activations = [];
					account_changes = [];
					payments = [];
					
					run(result.lastLedgerIndex + 1);
				}).catch(error => {
					console.log("Graph database commit: Error: " + error);
					process.exit(1);
				});
			}
			else {
				if(!result.done) {
					run(result.lastLedgerIndex + 1);
				}
				else {
					console.log("Backfiller is done");
					endScript();
				}
			}
		}).catch(error => {
			console.log(error);
		});
	}
	
	getStartLedger().then(result =>{
		console.log(`Initializing processing from ledger ${result.ledgerIndex}`)
		run(result.ledgerIndex);
	}).catch(error => {
		console.log("Could not get start ledger: " + error);
	});
	
	const endScript = () => {
		ledgerDB.close();
		transactionDB.close();
		closeGraphDBDriver();
	}
	
	process.on('SIGINT', function() {
		console.log("\nGracefully shutting down from SIGINT (Ctrl+C)");
		Stopped = true 
		endScript();
	})
}).catch(error => {
	console.log(error);
})