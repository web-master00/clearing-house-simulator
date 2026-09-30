# Clearing House Simulator

A concurrent state engine for clearing financial transfers. It uses atomic balances, lock-ordered settlement, and a sliding-window fraud guard so many virtual threads can move money without races or deadlocks.

Each transfer is an immutable `Transaction` between immutable account identities. Balances live in `AtomicLong` fields protected by a per-account `ReentrantLock`. Locks are always taken in sorted account-id order. A fourth high-volume attempt inside the fraud window is blocked before balances move.

## What it covers

- **Immutable domain models.** `Transaction` and `Account` are Java records, so identity and transfer payloads cannot change after creation.
- **Lock-ordered settlement.** Sorted lock acquisition prevents deadlock when many transfers touch the same accounts.
- **Sliding-window fraud guard.** High-volume attempts are counted per account; a fourth spike inside the window is intercepted before settlement.
- **Virtual-thread workers.** An `ExecutorService` of virtual threads runs parallel transfers while a `SwingWorker` keeps the UI thread free.

Useful as a prototype for core-banking settlement, payment clearing gateways, and real-time fraud checks under bursty traffic.

## Requirements

- JDK 21 or newer

## Run

```bash
javac ClearingHouseSimulator.java
java ClearingHouseSimulator --accounts 64 --threads 16 --max-transfer 50000 --volatility 0.35
```

Defaults are 64 accounts, 16 worker threads, a 50,000 cent transfer cap, volatility `0.35`, a 2 second fraud window, and a 1,000,000 cent starting balance. Volatility must be between 0 and 1. At least two accounts are required.

With no CLI arguments, the program opens a Swing window for account count, threads, transfer cap, and volatility.
