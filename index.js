const Client = require('rippled-ws-client')
const binarycodec = require('ripple-binary-codec');
const shared = require('./shared.js');

const commitSize = 10;


console.log('Connecting to rippled server');
new Client("wss://s2.ripple.com").then(Connection => {
	let Stopped = false
	
	var activations = [];
	var account_changes = [];
	var payments = [];

	const fetchLedgerTransactions = (ledger_index) => {
		if(ledger_index % 50 === 0) {
			console.log(ledger_index);
		}
		return new Promise((resolve, reject) => {
			return Connection.send({
				command: 'ledger',
				ledger_index: parseInt(ledger_index),
				transactions: true,
				expand: false, // Because expanded binary results do not contain transaction hashes, it has to be done in two queries. https://github.com/ripple/rippled/issues/2803
				binary: false
			}, 10).then(Result => {
				if(!Result.ledger.closed) {
					resolve({activations: [], account_changes: [], payments: [], done: true});
				}
				else if(typeof Result.ledger.transactions === 'undefined' || Result.ledger.transactions.length === 0) {
					resolve({activations: [], account_changes: [], payments: [], done: false});
				} else {
					return Connection.send({
							command: 'ledger',
							ledger_index: parseInt(ledger_index),
							transactions: true, // Because expanded binary results do not contain transaction hashes, it has to be done in two queries. https://github.com/ripple/rippled/issues/2803
							expand: true
						}, 10).then(Result => {
							let transactions = Result.ledger.transactions.map(Tx => {
								return Object.assign(Tx, {
									ClosingTime: new Date((Result.ledger.close_time + 946684800) * 1000).toISOString(),
									LedgerSeq: Result.ledger.ledger_index
								})
							})
					
							return prepareLedgerTransactions(transactions).then(result => {
								resolve({activations: result.activations, account_changes: result.account_changes, payments: result.payments, done: false});
							})
							
						}).catch(reject)
				}
				return
			}).catch(reject)
		})
	}
		
	const run = (ledgerIndex) => {
		fetchLedgerTransactions(ledgerIndex).then(result => {
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
				console.log(`Graph database commit: Total count ${activations.length + payments.length + account_changes.length} – next ledger ${ledgerIndex + 1}`)

				commitToServer(activations, account_changes, payments).then(commitResult => {
					console.log("Graph database commit: Done");
					
					// Empty collectors after commit
					activations = [];
					account_changes = [];
					payments = [];
					
					run(ledgerIndex + 1);
				}).catch(error => {
					console.log("Graph database commit: Error: " + error);
					process.exit(1);
				});
			}
			else {
				if(!result.done) {
					run(ledgerIndex + 1);
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
		Connection.close()
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
