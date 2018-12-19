const neo4j = require('neo4j-driver').v1;
const driver = neo4j.driver("bolt://localhost", neo4j.auth.basic("neo4j", "1234"));

const genesisWallets = ["r8TR1AeB1RDQFabM6i8UoFsRF5basqoHJ", "rB5TihdPbKgMrkFqrqUC3yLdE8hhv4BdeY", "rJYMACXJd1eejwzZA53VncYmiK2kZSBxyD", "rsjB6kHDBDUw7iB5A1EVDK1WmgmR6yFKpB", "rGRGYWLmSvPuhKm4rQV287PpJUgTB1VeD7", "rUzSNPtxrmeSTpnjsvaTuQvF2SQFPFSvLn", "rNRG8YAUqgsqoE5HSNPHTYqEGoKzMd7DJr", "r43mpEMKY1cVUX8k6zKXnRhZMEyPU9aHzR", "r9ssnjg97d86PxMrjVsCAX1xE9qg8czZTu", "rppWupV826yJUFd2zcpRGSjQHnAHXqe7Ny", "rB59DESmVnTwXd2SCy1G4ReVkP5UM7ZYcN", "rDCJ39V8yW39Ar3Pod7umxnrp24jATE1rt", "rf7phSp1ABzXhBvEwgSA7nRzWv2F7K5VM7", "rHDcKZgR7JDGQEe9r13UZkryEVPytV6L6F", "rUf6pynZ8ucVj1jC9bKExQ7mb9sQFooTPK", "rhWcbzUj9SVJocfHGLn58VYzXvoVnsU44u", "rnj8sNUBCw3J6sSstY9QDDoncnijFwH7Cs", "rLqQ62u51KR3TFcewbEbJTQbCuTqsg82EY", "rGow3MKvbQJvuzPPP4vEoohGmLLZ5jXtcC", "rUvEG9ahtFRcdZHi3nnJeFcJWhwXQoEkbi", "rBQQwVbHrkf8TEcW4h4MtE6EUyPQedmtof", "rKMhQik9qdyq8TDCYT92xPPRnFtuq8wvQK", "rLeRkwDgbPVeSakJ2uXC2eqR8NLWMvU3kN", "rsRpe4UHx6HB32kJJ3FjB6Q1wUdY2wi3xi", "rpWrw1a5rQjZba1VySn2jichsPuB4GVnoC", "rpGaCyHRYbgKhErgFih3RdjJqXDsYBouz3", "rKZig5RFv5yWAqMi9PtC5akkGpNtn3pz8A", "r3AthBf5eW4b9ujLoXNHFeeEJsK3PtJDea", "rNWzcdSkXL28MeKaPwrvR3i7yU6XoqCiZc", "ramPgJkA1LSLevMg2Yrs1jWbqPTsSbbYHQ", "rHrSTVSjMsZKeZMenkpeLgHGvY5svPkRvR", "rPFPa8AjKofbPiYNtYqSWxYA4A9Eqrf9jG", "r3WjZU5LKLmjh8ff1q2RiaPLcUJeSU414x", "rBY8EZDiCNMjjhrC7SCfaGr2PzGWtSntNy", "r43ksW5oFnW7FMjQXDqpYGJfUwmLan9dGo", "rwoE5PxARitChLgu6VrMxWBHN7j11Jt18x", "rMNKtUq5Z5TB5C4MJnwzUZ3YP7qmMGog3y", "rGqM8S5GnGwiEdZ6QRm1GThiTAa89tS86E", "rLBwqTG5ErivwPXGaAGLQzJ2rr7ZTpjMx7", "rhuCtPvq6jJeYF1S7aEmAcE5iM8LstSrrP", "r4HabKLiKYtCbwnGG3Ev4HqncmXWsCtF9F", "rDa8TxBdCfokqZyyYEpGMsiKziraLtyPe8", "rPrz9m9yaXT94nWbqEG2SSe9kdU4Jo1CxA", "rJ6VE6L87yaVmdyxa9jZFXSAdEFSoTGPbE", "r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV", "rHTxKLzRbniScyQFGMb3NodmxA848W8dKM", "rnp8kFTTm6KW8wsbgczfmv56kWXghPSWbK", "rf8kg7r5Fc8cCszGdD2jeUZt2FrgQd76BS", "rBJwwXADHqbwsp6yhrqoyt2nmFx9FB83Th", "rMNzmamctjEDqgwyBKbYfEzHbMeSkLQfaS", "rHSTEtAcRZBg1SjcR4KKNQzJKF3y86MNxT", "rEe6VvCzzKU1ib9waLknXvEXywVjjUWFDN", "rJZCJ2jcohxtTzssBPeTGHLstMNEj5D96n", "rQsiKrEtzTFZkQjF9MrxzsXHCANZJSd1je", "rHXS898sKZX6RY3WYPo5hW6UGnpBCnDzfr", "rPcHbQ26o4Xrwb2bu5gLc3gWUsS52yx1pG", "r3PDtZSa5LiYp1Ysn1vMuMzB59RzV3W9QH", "rhdAw3LiEfWWmSrbnZG3udsN7PoWKT56Qo", "rLs1MzkFWCxTbuAHgjeTZK4fcCDDnf2KRv", "rUnFEsHjxqTswbivzL2DNHBb34rhAgZZZK", "r4mscDrVMQz2on2px31aV5e5ouHeRPn8oy", "rLCvFaWk9WkJCCyg9Byvwbe9gxn1pnMLWL", "rLzpfV5BFjUmBs8Et75Wurddg4CCXFLDFU", "rUy6q3TxE4iuVWMpzycrQfD5uZok51g1cq", "rMwNkcpvcJucoWbFW89EGT6TfZyGUkaGso", "rPhMwMcn8ewJiM6NnP6xrm9NZBbKZ57kw1", "rnT9PFSfAnWyj2fd7D5TCoCyCYbK4n356A", "rEyhgkRqGdCK7nXtfmADrqWYGT6rSsYYEZ", "rJFGHvCtpPrftTmeNAs8bYy5xUeTaxCD5t", "rNSnpURu2o7mD9JPjaLsdUw2HEMx5xHzd", "rUZRZ2b4NyCxjHSQKiYnpBuCWkKwDWTjxw", "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59", "rauPN85FeNYLBpHgJJFH6g9fYUWBmJKKhs", "rEMqTpu21XNk62QjTgVXKDig5HUpNnHvij", "rDngjhgeQZj9FNtW8adgHvdpMJtSBMymPe", "rEJkrunCP8hpvk4ijxUgEWnxCE6iUiXxc2", "rLCAUzFMzKzcyRLa1B4LRqEMsUkYXX1LAs", "r4cmKj1gK9EcNggeHMy1eqWakPBicwp69R", "rnNPCm97TBMPprUGbfwqp1VpkfHUqMeUm7", 
"rwZpVacRQHYArgN3NzUfuKEcRDfbdvqGMi", "rfCXAzsmsnqDvyQj2TxDszTsbVj5cRTXGM", "rfpQtAXgPpHNzfnAYykgT6aWa94xvTEYce", "r4U5AcSVABL6Ym85jB94KYnURnzkRDqh1Y", "rHzWtXTBrArrGoLDixQAgcSD2dBisM19fF", "r9hEDb4xBGRfBCcX3E4FirDWQBAYtpxC8K", "r2oU84CFuT4MgmrDejBaoyHNvovpMSPiA", "rVehB9r1dWghqrzJxY2y8qTiKxMgHFtQh", "rsQP8f9fLtd58hwjEArJz2evtrKULnCNif", "rMkq9vs7zfJyQSPPkS2JgD8hXpDR5djrTA", "r4q1ujKY4hwBpgFNFx43629f2LuViU4LfA", "rhDfLV1hUCanViHnjJaq3gF1R2mo6PDCSC", "rwDWD2WoU7npQKKeYd6tyiLkmr7DuyRgsz", "rBrspBLnwBRXEeszToxcDUHs4GbWtGrhdE", "rLebJGqYffmcTbFwBzWJRiv5fo2ccmmvsB", "rPWyiv5PXyKWitakbaKne4cnCQppRvDc5B", "rHWKKygGWPon9WSj4SzTH7vS4ict1QWKo9", "rGwUWgN5BEg3QGNY3RX2HfYowjUTZdid3E", "rKHD6m92oprEVdi1FwGfTzxbgKt8eQfUYL", "r9duXXmUuhSs6JxKpPCSh2tPUg9AGvE2cG", "rphasxS8Q5p5TLTpScQCBhh5HfJfPbM2M8", "rU5KBPzSyPycRVW1HdgCKjYpU6W9PKQdE8", "r4DGz8SxHXLaqsA9M2oocXsrty6BMSQvw3", "rBnmYPdB5ModK8NyDUad1mxuQjHVp6tAbk", "rfitr7nL7MX85LLKJce7E3ATQjSiyUPDfj", "rD1jovjQeEpvaDwn9wKaYokkXXrqo4D23x", "rJQx7JpaHUBgk7C56T2MeEAu1JZcxDekgH", "r9aRw8p1jHtR9XhDAE22TjtM7PdupNXhkx", "rM1oqKtfh1zgjdAgbFmaRm3btfGBX25xVo", "rPgrEG6nMMwAM1VbTumL23dnEX4UmeUHk7", "rLp9pST1aAndXTeUYFkpLtkmtZVNcMs2Hc", "rnziParaNb8nsU4aruQdwYE3j5jUcqjzFm", "rwpRq4gQrb58N7PRJwYEQaoSui6Xd3FC7j", "rMYBVwiY95QyUnCeuBQA1D47kXA9zuoBui", "rGLUu9LfpKyZyeTtSRXpU15e2FfrdvtADa", "rhxbkK9jGqPVLZSWPvCEmmf15xHBfJfCEy", "rHC5QwZvGxyhC75StiJwZCrfnHhtSWrr8Y", "r49pCti5xm7WVNceBaiz7vozvE9zUGq8z2", "rKdH2TKVGjoJkrE8zQKosL2PCvG2LcPzs5", "rBqCdAqw7jLH3EDx1Gkw4gUAbFqF7Gap4c", "rwCYkXihZPm7dWuPCXoS3WXap7vbnZ8uzB", "rnCiWCUZXAHPpEjLY1gCjtbuc9jM1jq8FD", "rp1xKo4CWEzTuT2CmfHnYntKeZSf21KqKq", "rDJvoVn8PyhwvHAWuTdtqkH4fuMLoWsZKG", "rEA2XzkTXi6sWRzTVQVyUoSX4yJAzNxucd", "rshceBo6ftSVYo8h5uNPzRWbdqk4W6g9va", "rBKPS4oLSaV2KVVuHH8EpQqMGgGefGFQs7", "rEUXZtdhEtCDPxJ3MAgLNMQpq4ASgjrV6i", "rnGTwRTacmqZZBwPB6rh3H1W4GoTZCQtNA", "rJRyob8LPaA3twGEQDPU2gXevWhpSgD8S6", "rJ51FBSh6hXSUkFdMxwmtcorjx9izrC1yj", "rnxyvrF2mUhK6HubgPxUfWExERAwZXMhVL", "rDsDR1pFaY8Ythr8px4N98bSueixyrKvPx", "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh", "rDy7Um1PmjPgkyhJzUWo1G8pzcDan9drox", "rLiCWKQNUs8CQ81m2rBoFjshuVJviSRoaJ", "rEWDpTUVU9fZZtzrywAUE6D6UcFzu6hFdE"]

const firstLedger = 32570;

function hex2a(hexx) {
	var hex = hexx.toString();
	var str = '';
	for (var i = 0; (i < hex.length && hex.substr(i, 2) !== '00'); i += 2)
		str += String.fromCharCode(parseInt(hex.substr(i, 2), 16));
	return str;
}

commitToServer = (activations, account_changes, payments) => {
	return new Promise((resolve, reject) => {
		var session = driver.session();
		var tx = session.beginTransaction();

		// Create wallets and activations
		activations.forEach(activation => {
			tx.run(`MERGE (wallet:Wallet {address: {addressParam} })
			ON CREATE SET wallet.address = {addressParam}`,
			{
				addressParam: activation.child
			})
		
			tx.run(`MATCH (parent:Wallet {address: {parentParam} })
			MATCH (child:Wallet {address: {childParam} })
			MERGE (parent)-[activation:HAS_ACTIVATED]->(child)
			ON CREATE SET activation.date = datetime({dateParam}), activation.hash = {hashParam}, activation.ledgerIndex = {ledgerIndexParam}`, 
			{
				childParam: activation.child,
				parentParam: activation.parent,
				dateParam: activation.date,
				hashParam: activation.hash,
				ledgerIndexParam: neo4j.int(activation.ledger_index)
			})
		})
	
		// Update wallets from AccountSet transactions
		account_changes.forEach(account_change => {
			if(typeof account_change.domain !== 'undefined') {
				tx.run(`MERGE (wallet:Wallet { address: {addressParam} })
				ON MATCH SET wallet.domain = {domainParam}`, 
				{
					addressParam: account_change.account,
					domainParam: account_change.domain
				})
			}
		})
	
		// Create payments
		payments.forEach(payment => {
			tx.run(`MATCH (sender:Wallet {address: {senderParam} })
			MATCH (receiver:Wallet {address: {receiverParam} })
			MERGE (sender)-[:HAS_SENT]->(po:Payment { hash: {hashParam} })-[:HAS_RECEIVED]->(receiver)
			ON CREATE SET po.date = datetime({dateParam}), po.currency = {currencyParam}, po.issuer = {issuerParam}, po.amount = {amountParam}, po.fee = {feeParam}, po.memos = {memosParam}, po.hash = {hashParam}, po.ledgerIndex = {ledgerIndexParam}, po.destinationTag = {destinationTagParam}, po.flags = {flagsParam}, po.sourceTag = {sourceTagParam}, po.invoiceID = {invoiceIDParam}, po.sentCurrency = {sentCurrencyParam}, po.sentIssuer = {sentIssuer}, po.sentAmount = {sentAmountParam}`, 
			{
				senderParam: payment.sender,
				receiverParam: payment.receiver,
				destinationTagParam: payment.destination_tag ? neo4j.int(payment.destination_tag) : null,
				hashParam: payment.hash,
				dateParam: payment.date,
				ledgerIndexParam: neo4j.int(payment.ledger_index),
				currencyParam: payment.amount.currency,
				issuerParam: payment.amount.issuer ? payment.amount.issuer : null,
				amountParam: payment.amount.value,
				sentAmountParam: payment.sent_amount.value ? payment.sent_amount.value : null,
				sentIssuer: payment.sent_amount.issuer ? payment.sent_amount.issuer : null,
				sentCurrencyParam: payment.sent_amount.currency ? payment.sent_amount.currency : null,
				feeParam: payment.fee,
				memosParam: payment.memos ? payment.memos : null,
				flagsParam: payment.flags ? neo4j.int(payment.flags) : null,
				sourceTagParam: payment.source_tag ? neo4j.int(payment.source_tag) : null,
				invoiceIDParam: payment.invoice_id ? payment.invoice_id : null
			})
		})

		return tx.commit().subscribe({
			onCompleted: function (result) {
				session.close();
				resolve(result);
			},
			onError: function (error) {
				reject(error);
			}
		});
	});
}

initialSetup = () => {
	return new Promise((resolve, reject) => {
		console.log("Initial setup: Creating graph database indexes");
		
		var session = driver.session();
		var tx = session.beginTransaction();

		tx.run("CREATE CONSTRAINT ON (o:Wallet) ASSERT o.address IS UNIQUE");
		tx.run("CREATE CONSTRAINT ON (o:Payment) ASSERT o.hash IS UNIQUE");
		tx.run("CREATE INDEX ON :Payment(ledgerIndex)");
		tx.run("CREATE INDEX ON :Payment(date)");
		tx.run("CREATE INDEX ON :Payment(amount)");

		return tx.commit().then(function (result) {
			
			console.log("Initial setup: Graph database indexes done. Creating genesis wallets");
			
			var tx = session.beginTransaction();

			genesisWallets.forEach(genesisWallet => {
				tx.run(`MERGE (wallet:Wallet {address: {addressParam} })
				ON CREATE SET wallet.address = {addressParam}`,
				{
					addressParam: genesisWallet
				})	
			})

			return tx.commit().then(function (result) {
				session.close();
				resolve({ledgerIndex:firstLedger});
			}).catch(reject);

		}).catch(reject);
	});
}

prepareLedgerTransactions = (transactions) => {
	return new Promise((resolve, reject) => {
		var activations = [];
		var account_changes = [];
		var payments = [];
	
		transactions.forEach(Tx => {
			if(Tx.metaData.TransactionResult === "tesSUCCESS") {
				if(Tx.TransactionType === "Payment") {
					var payment = {
						sender: Tx.Account,
						receiver: Tx.Destination,
						ledger_index: Tx.LedgerSeq,
						date: Tx.ClosingTime,
						hash: Tx.hash,
						fee: parseFloat(Tx.Fee / 1000000)
					}
            	
					if(typeof Tx.Memos !== 'undefined') {
						var memos = [];
						Tx.Memos.forEach(memo => {
							var memoLines = []
							if(typeof memo.Memo === 'object') {
								if(typeof memo.Memo.MemoType !== 'undefined') {
									memoLines.push("MemoType: " + hex2a(memo.Memo.MemoType));
								}
								if(typeof memo.Memo.MemoFormat !== 'undefined') {
									memoLines.push("MemoFormat: " + hex2a(memo.Memo.MemoFormat));
								}
								if(typeof memo.Memo.MemoData !== 'undefined') {
									memoLines.push("MemoData: " + hex2a(memo.Memo.MemoData));
								}
							}
							memos.push(memoLines.join('\n'))
						})
						if(memos.length > 0) {
							payment.memos = memos.join('\n\n');
						}
					}
            	
					if(typeof Tx.SourceTag !== 'undefined') {
						payment.source_tag = parseInt(Tx.SourceTag);
					}
						
					if(typeof Tx.Flags !== 'undefined') {
						payment.flags = parseInt(Tx.Flags);
					}
						
					if(typeof Tx.InvoiceID !== 'undefined') {
						payment.invoice_id = Tx.InvoiceID;
					}
															
					if(typeof Tx.DestinationTag !== 'undefined') {
						payment.destination_tag = parseInt(Tx.DestinationTag);
					}
            	
					if(typeof Tx.Amount === 'string') {
						payment.amount = {
							currency: "XRP",
							value: parseFloat(Tx.Amount / 1000000)
						}
					} 
					else if(typeof Tx.Amount === 'object' && typeof Tx.Amount.currency !== 'undefined') {
						payment.amount = {
							currency: Tx.Amount.currency,
							issuer: Tx.Amount.issuer,
							value: parseFloat(Tx.Amount.value)
						}
					}
				
					payment.sent_amount = payment.amount;
            	
					if (typeof Tx.metaData.DeliveredAmount === 'undefined' && typeof Tx.metaData.delivered_amount !== 'undefined') {
						if(typeof Tx.metaData.delivered_amount === 'string') {
							payment.amount = {
								currency: "XRP",
								value: parseFloat(Tx.metaData.delivered_amount / 1000000)
							}
						}
						else if(typeof Tx.metaData.delivered_amount === 'object' && typeof Tx.metaData.delivered_amount.currency !== 'undefined') {
							payment.amount = {
								currency: Tx.metaData.delivered_amount.currency,
								issuer: Tx.metaData.delivered_amount.issuer,
								value: parseFloat(Tx.metaData.delivered_amount.value)
							}
						}
					}
					else if (typeof Tx.metaData.DeliveredAmount !== 'undefined') {
						if(typeof Tx.metaData.DeliveredAmount === 'string') {
							payment.amount = {
								currency: "XRP",
								value: parseFloat(Tx.metaData.DeliveredAmount / 1000000)
							}
						}
						else if(typeof Tx.metaData.DeliveredAmount === 'object' && typeof Tx.metaData.DeliveredAmount.currency !== 'undefined') {
							payment.amount = {
								currency: Tx.metaData.DeliveredAmount.currency,
								issuer: Tx.metaData.DeliveredAmount.issuer,
								value: parseFloat(Tx.metaData.DeliveredAmount.value)
							}
						}
					}
            	
					// Not dealing with bridging/payment paths
            	
					Tx.metaData.AffectedNodes.forEach(affectedNode => {
						if (typeof affectedNode.CreatedNode !== 'undefined' && affectedNode.CreatedNode.LedgerEntryType === 'AccountRoot') {
							var activation = {
								parent: Tx.Account,
								child: affectedNode.CreatedNode.NewFields.Account,
								ledger_index: Tx.LedgerSeq,
								date: Tx.ClosingTime,
								hash: Tx.hash
							}
							activations.push(activation);
						}
					})
            	
					payments.push(payment);
				}
				else if(Tx.TransactionType === "AccountSet") {
					Tx.metaData.AffectedNodes.forEach(affectedNode => {
						if (typeof affectedNode.ModifiedNode !== 'undefined' && affectedNode.ModifiedNode.LedgerEntryType === 'AccountRoot') {
							var account_change = {
								account: affectedNode.ModifiedNode.FinalFields.Account
							}
            	
							if(typeof affectedNode.ModifiedNode.FinalFields.Domain !== 'undefined') {
								account_change.domain = hex2a(affectedNode.ModifiedNode.FinalFields.Domain);
							}
            	
							account_changes.push(account_change);
						}
					})
				}
			}
		});
		
		resolve({activations:activations, account_changes:account_changes, payments:payments});
	});
}

getStartLedger = () => {
	return new Promise((resolve, reject) => {
		var session = driver.session();
		session.run('MATCH (p:Payment) RETURN p.ledgerIndex ORDER BY p.ledgerIndex DESC LIMIT 1')
		.then(function (result) {
			if(result.records.length === 0) {
				initialSetup().then(resolve).catch(reject);
			}
			else {
				resolve({ledgerIndex:result.records[0].get('p.ledgerIndex').low})
			}
		}).catch(reject);
	});
}

closeGraphDBDriver = () => {
	driver.close();
}
