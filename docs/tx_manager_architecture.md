# TxManager Architecture Overview

`TxManager` is a library to manage Ethereum transactions and make sure they are eventually included in the canonical
chain. The pending transactions are persisted in local storage for failure recovery. We adapted the design and
architecture from [Chainlink](https://github.com/smartcontractkit/chainlink/tree/931f56d6ec6029157e7770ffadecb6657c128f8e/core/services/bulletprooftxmanager). We refactored the codebase to better abstract the storage interface and allow
different database implementations.

## Data structures

### Account

`Account` represents an Ethereum account. The data structure keeps track of a `NextNonce` representing the nonce to be
used for the next transaction. `NextNonce` is only updated after a successful broadcast has occurred. Each `Account`
also maintains the list of transaction requests submitted.

`TxManager` is able to handle multiple accounts concurrently.

### Tx

`Tx` represents an Ethereum transaction request, containing all the metadata required to broadcast the transaction. Each
`Tx` maintains a state in the lifecycle along with a list of `TxAttempt`s.

### TxAttempt

`TxAttempt` represents a transaction attempt that is broadcast to the Ethereum network. It may have 0 or more
`TxReceipt`s indicating that the transaction has been mined into a block.

### TxReceipt

`TxReceipt`s are saved for each `TxAttempt` once it is mined into a block. Note that this block may or may not be part
of the canonical / longest chain, so multiple receipts can exist for a single `TxAttempt`.

## Components and state transitions

`TxManager` is made up of three components: `HeadTracker`, `TxBroadcaster`, and `TxConfirmer`, each of which has a clear
set of responsibilities.

Simple state transition diagrams are as follow:

⚫️ - has never been broadcast to the network

🟠 - may or may not have been broadcast to the network

🔵 - has definitely been broadcast to the network

TB - `TxBroadcaster`

TC - `TxConfirmer`

`Tx` has five possible states:

- TB ⚫️ `unstarted`
- TB 🟠 `in_progress`
- TB/TC ⚫️ `fatal_error`
- TB/TC 🔵 `unconfirmed`
- TB/TC 🔵 `confirmed`

```
unstarted
|
|
v
in_progress (only one per account)
|                  \
|                   \
v                    v
fatal_error     unconfirmed
                    |   ^
                    |   |
                    v   |
                  confirmed
```

`TxAttempt` has two possible states:

- TB/TC 🟠 `in_progress`
- TB/TC 🔵 `broadcast`

```
in_progress
|   ^
|   |
v   |
broadcast
```

## Operations

### HeadTracker

`HeadTracker` subscribes to an Ethereum RPC endpoint via WebSocket. It maintains a chain of block headers in the
database, detecting and tracking re-orgs up to a configured finality depth. `TxConfirmer` subscribes to `HeadTracker`
via the `HeadTrackable` interface.

### TxBroadcaster

`TxBroadcaster` monitors the `Txs` table for new transaction requests. It assigns nonces and ensures that the
transaction is broadcast to the Ethereum network. Note that it does not guarantee eventual confirmation. After the
initial broadcast, many things can subsequently go wrong such as transactions being evicted from the mempool, Ethereum
nodes crashing, network splits between nodes, chain re-orgs, etc. `TxConfirmer` will take over and ensure the eventual
inclusion of the transaction into the longest chain.

`TxBroadcaster` serializes access to each `Account` but can be run for multiple `Account`s in parallel.

`TxBroadcaster` makes the following guarantees:

- For each `Account`, a gapless, monotonically increasing sequence of nonces for `Tx`s.
- Transition of `Tx` from `unstarted` to either `fatal_error` or `unconfirmed`.
- If the final state is `fatal_error`, then the nonce is unassigned and it is impossible that this transaction could
ever be mined into a block.
- If the final state is `unconfirmed` then a `TxAttempt` is persisted for the `Tx`.
- If the final state is `unconfirmed` then an Ethereum node somewhere has accepted this transaction into its mempool at
least once.

### TxConfirmer

`TxConfirmer` has four tasks:

1. Set `TxAttempt.BroadCastBeforeBlockNum`

This field indicates how long a transaction has been waiting for inclusion into a block and we need this information to
decide if we should bump the gas.

When `HeadTracker` receives a new block, we can be sure that any currently `unconfirmed` transactions were broadcast
before this block was received, so we set `BroadCastBeforeBlockNum` for all `TxAttempt`s made since we saw the last
block.

2. Check for receipts

Find all `unconfirmed` `Tx`s and ask the Ethereum node for a receipt. If there is a receipt, we save it and move
this `Tx` into `confirmed` state. Note that this state doesn't imply finality is achieved.

3. Bump gas if necessary

Find all `unconfirmed` `Tx`s where all `TxAttempt`s have remained unconfirmed for more than `Config.GasBumpThreshold`
number of blocks. Create a new `TxAttempt` for each with a higher gas price and broadcast it.

4. Re-org protection

Find all `Tx`s confirmed within the past `Config.FinalityDepth` blocks and verify that they have at least one
receipt in the current longest chain. Rebroadcast the ones that don't meet the criterion.

`TxConfirmer` makes the following guarantees:

- All transactions will eventually be confirmed on the canonical longest chain, unless a reorg occurs that is deeper
than `Config.FinalityDepth` blocks.
- In the case that an external wallet used the nonce, we will ensure that *a* transaction exists at this nonce up to a
depth of `Config.FinalityDepth` blocks but it most likely will not be a transaction handled by `TxManager`.
