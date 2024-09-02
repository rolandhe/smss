package backgroud

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/tm"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"os"
	"path"
	"slices"
	"strings"
	"time"
)

const (
	DeleteFileAfterStateChangeTimeout = int64(1000 * 60 * 5)
)

var cronIns *cron.Cron

func StartClearOldFiles(root string, store store.Store, worker standard.MessageWorking, delTopicFileExecutor protocol.DelTopicFileExecutor) {
	cronIns = cron.New()

	// 添加定时任务
	cronIns.AddFunc(fmt.Sprintf("@every %ds", conf.StoreClearInterval), func() {
		deleteOldFiles(root, store, worker, delTopicFileExecutor)
	})

	// 启动 cron 调度器
	cronIns.Start()
	logger.Infof("StartClearOldFiles run ok")
}

func StopClear() {
	if cronIns == nil {
		return
	}
	cronIns.Stop()
	cronIns = nil
}

func deleteOldFiles(root string, fstore store.Store, worker standard.MessageWorking, delTopicFileExecutor protocol.DelTopicFileExecutor) {
	traceId := fmt.Sprintf("delOldFile-%d", time.Now().UnixMilli())

	logger.Infof("tid=%s,run deleteOldFiles", traceId)

	deleteBinlogFiles(traceId, path.Join(root, store.BinlogDir))

	infoList, err := fstore.GetTopicInfoReader().GetTopicSimpleInfoList()
	if err != nil {
		logger.Infof("deleteOldFiles err:%v", err)
		return
	}

	locker := delTopicFileExecutor.GetDeleteFileLocker()

	for _, info := range infoList {
		p := fstore.GetTopicPath(info.Name)
		if store.TopicStateNormal == info.State {
			for {
				unLockFunc, waiter := locker.Lock(info.Name, "ClearOldFiles", traceId)
				if unLockFunc != nil {
					deleteInvalidFiles(p, unLockFunc, traceId, "topic")
					break
				}
				if !waiter(time.Second * 3) {
					break
				}
			}
			continue
		}
		if store.TopicStateDeleted == info.State {
			if info.StateChangeTime+DeleteFileAfterStateChangeTimeout <= time.Now().UnixMilli() {
				err = deleteTopic(worker, info.Name, traceId)
				logger.Infof("tid=%s,deleteOldFiles to delete expired topic %s err:%v", traceId, info.Name, err)
			}
			continue
		}
	}
}

func deleteBinlogFiles(traceId, binlogPath string) {
	deleteInvalidFiles(binlogPath, nil, traceId, "binlog")
}

func deleteInvalidFiles(p string, unLockFunc func(), traceId string, scenario string) {
	if unLockFunc != nil {
		defer unLockFunc()
	}
	_, err := os.Stat(p)
	if os.IsNotExist(err) {
		return
	}
	entries, err := os.ReadDir(p)
	if err != nil {
		logger.Infof("tid=%s,%s,read dir:%s error:%v", traceId, scenario, p, err)
		return
	}

	nowDate := tm.NowDate()
	var maxId int64
	var delIds []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		items := strings.Split(name, ".")
		if len(items) != 2 || items[1] != "log" {
			logger.Infof("tid=%s,%s,file %s not valid log file", traceId, scenario, name)
			continue
		}
		num := dir.ParseNumber(items[0])
		if num > maxId {
			maxId = num
		}

		info, err := entry.Info()
		if err != nil {
			logger.Infof("tid=%s,%s,read dir:%s/%s error:%v", traceId, scenario, p, entry.Name(), err)
			continue
		}
		modDate := tm.ToDate(info.ModTime())
		if tm.DiffDays(nowDate, modDate) >= conf.StoreMaxDays {
			delIds = append(delIds, num)
		}
	}

	slices.Sort(delIds)
	l := len(delIds)
	if l > 0 && delIds[l-1] == maxId {
		delIds = delIds[:l-1]
		logger.Infof("tid=%s,%s,deleteInvalidFiles,retain the last expired file:%s/%d.log", traceId, scenario, p, maxId)
	}

	logger.Infof("tid=%s,%s,to delete expired files:%d", traceId, scenario, len(delIds))

	for _, id := range delIds {
		dp := fmt.Sprintf("%s/%d.log", p, id)
		err = os.Remove(dp)
		logger.Infof("tid=%s,%s,delete %s err:%v", traceId, scenario, dp, err)
	}
}
