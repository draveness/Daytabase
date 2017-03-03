//
//  ConnectionState.swift
//  Daytabase
//
//  Created by Draveness on 3/3/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation

struct ConnectionState {
    weak var connection: Connection?

    let activeReadTransaction: Bool = false
    let longLivedReadTransaction: Bool = false
    let sqlLevelSharedReadLock: Bool = false

    let activeWriteTransaction: Bool = false
    let waitingForWriteLock: Bool = false

    let lastTransactionSnapshot: Int64 = 0
    let lastTransactionTime: Int64 = 0

    private let writeSemaphore = DispatchSemaphore(value: 0)

    func waitForWriteLock() {
        writeSemaphore.wait()
    }

    func signalWriteLock() {
        writeSemaphore.signal()
    }
}
