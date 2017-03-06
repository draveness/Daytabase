//
//  Constant.swift
//  Daytabase
//
//  Created by Draveness on 3/3/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation
import CSQLite3

let SQLITE_COLUMN_START: Int32 = 0
let SQLITE_BIND_START: Int32 = 1

let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)

let extensionTableName = "yap2"
let defaultTableName = "database2"

let defaultSerializer: DatabaseSerializer = { (collection: String, key: String, value: Any) in
    return NSKeyedArchiver.archivedData(withRootObject: value)
}

let defaultDeserializer: DatabaseDeserializer = { (collection: String, key: String, data: Data) in
    return data.count > 0 ? NSKeyedUnarchiver.unarchiveObject(with: data) : nil
}

func daytabase_errmsg(_ db: OpaquePointer?) -> String {
    return String(cString: sqlite3_errmsg(db))
}
