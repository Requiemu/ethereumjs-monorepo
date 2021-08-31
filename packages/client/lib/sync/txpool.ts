import Heap from 'qheap'
import {
  Capability,
  FeeMarketEIP1559Transaction,
  Transaction,
  TransactionFactory,
  TypedTransaction,
} from '@ethereumjs/tx'
import { BN } from 'ethereumjs-util'
import { Config } from '../config'
import { Peer } from '../net/peer'
import { EthProtocolMethods } from '../net/protocol'
import type { Block } from '@ethereumjs/block'
import { PeerPool } from '../net/peerpool'

export interface TxPoolOptions {
  /* Config */
  config: Config
}

type TxPoolObject = {
  tx: TypedTransaction
  hash: string // plain strings without hex prefix
  added: number
}

type HandledObject = {
  address: string // plain strings without hex prefix
  added: number
}

type UnprefixedAddress = string
type UnprefixedHash = string

/**
 * @module service
 */

/**
 * Tx pool (mempool)
 * @memberof module:service
 */
export class TxPool {
  public config: Config

  private opened: boolean
  public running: boolean

  /* global NodeJS */
  private _logInterval: NodeJS.Timeout | undefined

  /**
   * List of pending tx hashes to avoid double requests
   */
  private pending: UnprefixedHash[] = []

  /**
   * Map for handled tx hashes
   * (have been added to the pool at some point)
   *
   * This is meant to be a superset of the tx pool
   * so at any point it time containing minimally
   * all txs from the pool.
   */
  private handled: Map<UnprefixedHash, HandledObject>

  /**
   * The central pool dataset.
   *
   * Maps an address to a `TxPoolObject`
   */
  public pool: Map<UnprefixedAddress, TxPoolObject[]>

  /**
   * Activate before chain head is reached to start
   * tx pool preparation (sorting out included txs)
   */
  public BLOCKS_BEFORE_TARGET_HEIGHT_ACTIVATION = 20

  /**
   * Max number of txs to request
   */
  private TX_RETRIEVAL_LIMIT = 256

  /**
   * Number of minutes to keep txs in the pool
   */
  public POOLED_STORAGE_TIME_LIMIT = 20

  /**
   * Number of minutes to forget about handled
   * txs (for cleanup/memory reasons)
   */
  public HANDLED_CLEANUP_TIME_LIMIT = 60

  /**
   * Log pool statistics on the given interval
   */
  private LOG_STATISTICS_INTERVAL = 10000 // ms

  /**
   * Create new tx pool
   * @param options constructor parameters
   */
  constructor(options: TxPoolOptions) {
    this.config = options.config

    this.pool = new Map<UnprefixedAddress, TxPoolObject[]>()
    this.handled = new Map<UnprefixedHash, HandledObject>()

    this.opened = false
    this.running = false
  }

  /**
   * Open pool
   */
  open(): boolean {
    if (this.opened) {
      return false
    }
    this.opened = true

    return true
  }

  /**
   * Start tx processing
   */
  start(): boolean {
    if (this.running) {
      return false
    }
    this._logInterval = setInterval(this._logPoolStats.bind(this), this.LOG_STATISTICS_INTERVAL)
    this.running = true
    this.config.logger.info('TxPool started.')
    return true
  }

  /**
   * Adds a tx to the pool.
   *
   * If there is a tx in the pool with the same address and
   * nonce it will be replaced by the new tx.
   * @param tx Transaction
   */
  add(tx: TypedTransaction) {
    const sender: UnprefixedAddress = tx.getSenderAddress().toString()
    const inPool = this.pool.get(sender)
    let add: TxPoolObject[] = []
    if (inPool) {
      // Replace pooled txs with the same nonce
      add = inPool.filter((poolObj) => !poolObj.tx.nonce.eq(tx.nonce))
    }
    const address: UnprefixedAddress = tx.getSenderAddress().toString()
    const hash: UnprefixedHash = tx.hash().toString('hex')
    const added = Date.now()
    add.push({ tx, added, hash })
    this.pool.set(address, add)

    this.handled.set(hash, { address, added })
  }

  /**
   * Removes the given tx from the pool
   * @param txHash Hash of the transaction
   */
  removeByHash(txHash: UnprefixedHash) {
    if (!this.handled.has(txHash)) {
      return
    }
    const address = this.handled.get(txHash)!.address
    if (!this.pool.has(address)) {
      return
    }
    const newPoolObjects = this.pool.get(address)!.filter((poolObj) => poolObj.hash !== txHash)
    if (newPoolObjects.length === 0) {
      // List of txs for address is now empty, can delete
      this.pool.delete(address)
    } else {
      // There are more txs from this address
      this.pool.set(address, newPoolObjects)
    }
  }

  /**
   * Send transactions to other peers in the peer pool
   * @param pool
   * @param tx Array with transactions to send
   */
  sendTransactions(peerPool: PeerPool, txs: TypedTransaction[]) {
    const peers = peerPool.peers

    for (const peer of peers) {
      const txsToSend = []
      for (const tx of txs) {
        // TODO: check if tx has already been sent to peer
        if (tx.type === 0) {
          txsToSend.push(tx.raw())
        } else {
          txsToSend.push(tx.serialize())
        }
      }
      if (txsToSend.length > 0) {
        peer.eth?.send('Transactions', txsToSend)
      }
    }
  }

  /**
   * Include new pooled txs announced in the pool
   * @param  txHashes new tx hashes announced
   * @param  peer peer
   */
  async includeAnnouncedTxs(txHashes: Buffer[], peer: Peer) {
    if (!this.running || txHashes.length === 0) {
      return
    }
    this.config.logger.debug(`TxPool: received new pooled hashes number=${txHashes.length}`)

    this.cleanup()

    const reqHashes = []
    for (const txHash of txHashes) {
      const txHashStr: UnprefixedHash = txHash.toString('hex')
      if (this.pending.includes(txHashStr) || this.handled.has(txHashStr)) {
        continue
      }
      reqHashes.push(txHash)
    }

    if (reqHashes.length === 0) {
      return
    }

    const reqHashesStr: UnprefixedHash[] = reqHashes.map((hash) => hash.toString('hex'))
    this.pending.concat(reqHashesStr)
    this.config.logger.debug(
      `TxPool: requesting txs number=${reqHashes.length} pending=${this.pending.length}`
    )
    const txsResult = await (peer!.eth as EthProtocolMethods).getPooledTransactions({
      hashes: reqHashes.slice(0, this.TX_RETRIEVAL_LIMIT),
    })

    this.config.logger.debug(`TxPool: received txs number=${txsResult[1].length}`)

    // Remove from pending list regardless if tx is in result
    for (const reqHashStr of reqHashesStr) {
      this.pending = this.pending.filter((hash) => hash !== reqHashStr)
    }

    for (const txData of txsResult[1]) {
      const tx = TransactionFactory.fromBlockBodyData(txData)
      this.add(tx)
    }
  }

  /**
   * Remove txs included in the latest blocks from the tx pool
   */
  removeNewBlockTxs(newBlocks: Block[]) {
    if (!this.running) {
      return
    }
    for (const block of newBlocks) {
      for (const tx of block.transactions) {
        const txHash: UnprefixedHash = tx.hash().toString('hex')
        this.removeByHash(txHash)
      }
    }
  }

  /**
   * Regular tx pool cleanup
   */
  cleanup() {
    // Remove txs older than POOLED_STORAGE_TIME_LIMIT from the pool
    let compDate = Date.now() - this.POOLED_STORAGE_TIME_LIMIT * 60
    this.pool.forEach((poolObjects, address) => {
      const newPoolObjects = poolObjects.filter((obj) => obj.added >= compDate)
      if (newPoolObjects.length < poolObjects.length) {
        if (newPoolObjects.length === 0) {
          this.pool.delete(address)
        } else {
          this.pool.set(address, newPoolObjects)
        }
      }
    })

    // Cleanup handled txs
    compDate = Date.now() - this.HANDLED_CLEANUP_TIME_LIMIT * 60
    this.handled.forEach((handleObj, address) => {
      if (handleObj.added < compDate) {
        this.handled.delete(address)
      }
    })
  }

  /**
   * Helper to return a normalized gas price across different transaction types.
   * @param tx The tx
   * @param priority If a 1559 tx and true, returns the maxPriorityFeePerGas. If false, returns the maxFeePerGas. (default: true)
   */
  private txGasPrice(tx: TypedTransaction, priority = true) {
    if (tx.supports(Capability.EIP1559FeeMarket)) {
      if (priority) {
        return (tx as FeeMarketEIP1559Transaction).maxPriorityFeePerGas
      } else {
        return (tx as FeeMarketEIP1559Transaction).maxFeePerGas
      }
    } else {
      return (tx as Transaction).gasPrice
    }
  }

  /**
   * Returns eligible txs to be mined sorted by price in such a way that the
   * nonce orderings within a single account are maintained.
   *
   * Note, this is not as trivial as it seems from the first look as there are three
   * different criteria that need to be taken into account (price, nonce, account
   * match), which cannot be done with any plain sorting method, as certain items
   * cannot be compared without context.
   *
   * This method first sorts the separates the list of transactions into individual
   * sender accounts and sorts them by nonce. After the account nonce ordering is
   * satisfied, the results are merged back together by price, always comparing only
   * the head transaction from each account. This is done via a heap to keep it fast.
   *
   * @param baseFee Provide a baseFee to exclude txs with a lower gasPrice
   */
  txsByPriceAndNonce(baseFee?: BN) {
    const txs: TypedTransaction[] = []
    // Separate the transactions by account and sort by nonce
    const byNonce = new Map<string, TypedTransaction[]>()
    this.pool.forEach((poolObjects, address) => {
      let txsSortedByNonce = poolObjects
        .map((obj) => obj.tx)
        .sort((a, b) => a.nonce.sub(b.nonce).toNumber())
      if (baseFee) {
        // If any tx has an insiffucient gasPrice,
        // remove all txs after that since they cannot be executed
        const found = txsSortedByNonce.findIndex((tx) => this.txGasPrice(tx, false).lt(baseFee))
        if (found > -1) {
          txsSortedByNonce = txsSortedByNonce.slice(0, found)
        }
      }
      byNonce.set(address, txsSortedByNonce)
    })
    // Initialize a price based heap with the head transactions
    const byPrice = new Heap<TypedTransaction>({
      comparBefore: (a: TypedTransaction, b: TypedTransaction) =>
        this.txGasPrice(b).sub(this.txGasPrice(a)).toNumber() < 0,
    })
    byNonce.forEach((txs, address) => {
      byPrice.insert(txs[0])
      byNonce.set(address, txs.slice(1))
    })
    // Merge by replacing the best with the next from the same account
    while (byPrice.length > 0) {
      // Retrieve the next best transaction by price
      const best = byPrice.remove()
      if (!best) break
      // Push in its place the next transaction from the same account
      const address = best.getSenderAddress().toString()
      const accTxs = byNonce.get(address)!
      if (accTxs.length > 0) {
        byPrice.insert(accTxs[0])
        byNonce.set(address, accTxs.slice(1))
      }
      // Accumulate the best priced transaction
      txs.push(best)
    }
    return txs
  }

  /**
   * Stop pool execution
   */
  stop(): boolean {
    if (!this.running) {
      return false
    }
    clearInterval(this._logInterval as NodeJS.Timeout)

    this.running = false
    this.config.logger.info('TxPool stopped.')
    return true
  }

  /**
   * Close pool
   */
  close() {
    this.pool.clear()
    this.opened = false
  }

  _logPoolStats() {
    let count = 0
    this.pool.forEach((poolObjects) => {
      count += poolObjects.length
    })
    this.config.logger.info(`TxPool Statistics transactions=${count} senders=${this.pool.size}`)
  }
}
