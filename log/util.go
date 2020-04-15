package log

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"

	"github.com/coreos/etcd/pkg/fileutil"
)

func segmentName(baseIndex uint64) string {
	return fmt.Sprintf("wal-%016x.log", baseIndex)
}

var pathReg = regexp.MustCompile(`^wal-([0-9a-fA-F]{16}).log$`)

func segmentsIn(dir string) ([]uint64, error) {
	files, err := fileutil.ReadDir(dir, fileutil.WithExt(".log"))
	if err != nil {
		return nil, err
	}

	result := make([]uint64, 0, len(files))
	for _, f := range files {
		m := pathReg.FindAllStringSubmatch(f, 1)
		if len(m) == 0 || len(m[0]) < 2 {
			continue
		}

		v, err := strconv.ParseUint(m[0][1], 16, 64)
		if err != nil {
			continue
		}

		result = append(result, v)
	}

	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })

	return result, nil
}

func searchSegmentIndex(offsets []uint64, index uint64) (uint64, error) {
	i := sort.Search(len(offsets), func(i int) bool { return offsets[i] > index })
	if i == 0 {
		return 0, fmt.Errorf("value is less than first index")
	}

	return offsets[i-1], nil
}
