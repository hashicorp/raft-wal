// Copyright IBM Corp. 2020, 2025
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"hash/crc32"
)

var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}
