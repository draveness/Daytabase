//
//  ConnectionDefaults.swift
//  Daytabase
//
//  Created by Draveness on 3/4/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation

public struct ConnectionDefaults {
    private static let DefaultObjectCacheLimit   = 250
    private static let DefaultMetadataCacheLimit = 250

    var objectCacheEnabled: Bool = true
    var objectCacheLimit: Int = DefaultObjectCacheLimit

    var metadataCacheEnabled: Bool = true
    var metadataCacheLimit: Int = DefaultMetadataCacheLimit

    var objectPolicy: DatabasePolicy = .containment
    var metadataPolicy: DatabasePolicy = .containment
}
