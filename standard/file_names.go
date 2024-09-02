package standard

import (
	"fmt"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/tm"
	"math"
	"os"
	"path"
	"strings"
)

func GenLogFileFullPath(root string, fc LogFileControl) (string, error) {
	fid, _ := fc.Get()

	fPath := path.Join(root, fmt.Sprintf("%d.log", fid))
	return fPath, nil
}
func ReadMaxFileId(root string) (int64, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, err
	}

	var maxId int64 = -1
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		items := strings.Split(name, ".")
		if len(items) != 2 || items[1] != "log" {
			logger.Infof("file %s not valid log file", name)
			continue
		}
		num := dir.ParseNumber(items[0])
		if num < 0 {
			continue
		}

		if num > maxId {
			maxId = num
		}
	}
	return maxId + 1, nil
}

func ReadFirstFileId(root string, lastFileId int64) (int64, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, err
	}

	var firstId int64 = math.MaxInt64
	nowDate := tm.NowDate()

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		items := strings.Split(name, ".")
		if len(items) != 2 || items[1] != "log" {
			logger.Infof("file %s not valid log file", name)
			continue
		}
		num := dir.ParseNumber(items[0])
		if num < 0 {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return 0, err
		}
		modDate := tm.ToDate(info.ModTime())
		if tm.DiffDays(nowDate, modDate) >= conf.StoreMaxDays {
			continue
		}

		if num < firstId {
			firstId = num
		}
	}
	if firstId == math.MaxInt64 {
		firstId = lastFileId
	}
	return firstId, nil
}
