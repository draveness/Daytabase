//
//  Connection+LongLivedTransaction.swift
//  Daytabase
//
//  Created by Draveness on 3/4/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation

extension Connection {
    func endLongLivedTransaction() -> [Notification] {
        return []
    }

    func preWriteQueue() {
        OSSpinLockLock(&self.lock)
        if writeQueueSuspended {
            self.database.writeQueue.resume()
            writeQueueSuspended = false
        }
        activeReadWriteTransaction = true
        OSSpinLockUnlock(&self.lock)
    }

    func postWriteQueue() {
        OSSpinLockLock(&self.lock)
        activeReadWriteTransaction = false
        OSSpinLockUnlock(&self.lock)
    }
}
