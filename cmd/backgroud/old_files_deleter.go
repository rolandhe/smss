package backgroud

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"os"
	"strings"
	"time"
)

const (
	DeleteFileAfterStateChangeTimeout = int64(1000 * 60 * 5)
)

var cronIns *cron.Cron

func StartClearOldFiles(store store.Store, worker standard.MessageWorking, delTopicFileExecutor protocol.DelTopicFileExecutor) {
	cronIns = cron.New()

	// 添加定时任务
	cronIns.AddFunc(fmt.Sprintf("@every %ds", conf.StoreClearInterval), func() {
		deleteOldFiles(store, worker, delTopicFileExecutor)
	})

	// 启动 cron 调度器
	cronIns.Start()
}

func StopClear() {
	if cronIns == nil {
		return
	}
	cronIns.Stop()
	cronIns = nil
}

func deleteOldFiles(fstore store.Store, worker standard.MessageWorking, delTopicFileExecutor protocol.DelTopicFileExecutor) {
	traceId := fmt.Sprintf("delOldFile-%d", time.Now().UnixMilli())
	infoList, err := fstore.GetTopicInfoReader().GetTopicSimpleInfoList()
	if err != nil {
		logger.Get().Infof("deleteOldFiles err:%v", err)
	}

	locker := delTopicFileExecutor.GetDeleteFileLocker()

	for _, info := range infoList {
		p := fstore.GetTopicPath(info.Name)
		if store.TopicStateNormal == info.State {
			for {
				unLockFunc, waiter := locker.Lock(info.Name, "ClearOldFiles", traceId)
				if unLockFunc != nil {
					deleteInvalidFiles(p, unLockFunc, traceId)
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
				logger.Get().Infof("tid=%s,deleteOldFiles to delete expired topic %s err:%v", traceId, info.Name, err)
			}
			continue
		}
	}
}

func deleteInvalidFiles(p string, unLockFunc func(), traceId string) {
	defer unLockFunc()
	_, err := os.Stat(p)
	if os.IsNotExist(err) {
		return
	}
	entries, err := os.ReadDir(p)
	if err != nil {
		logger.Get().Infof("tid=%s,read dir:%s error:%v", traceId, p, err)
		return
	}

	nowDate := toDate(time.Now())
	var maxId int64
	var delIds []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		items := strings.Split(name, ".")
		if len(items) != 2 || items[1] != "log" {
			logger.Get().Infof("tid=%s,file %s not valid log file", traceId, name)
			continue
		}
		num := dir.ParseNumber(items[0])
		if num > maxId {
			maxId = num
		}

		info, err := entry.Info()
		if err != nil {
			logger.Get().Infof("tid=%s,read dir:%s/%s error:%v", traceId, p, entry.Name(), err)
			continue
		}
		modDate := toDate(info.ModTime())
		if diffDays(nowDate, modDate) >= conf.StoreMaxDays {
			delIds = append(delIds, num)
		}
	}

	for _, id := range delIds {
		if id == maxId {
			continue
		}
		dp := fmt.Sprintf("%s/%d.log", p, id)
		if err = os.Remove(dp); err != nil {
			logger.Get().Infof("tid=%s,delete %s err:%v", traceId, dp, err)
		}
	}
}

func toDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
}

func diffDays(d1, d2 time.Time) int {
	d := d1.Sub(d2)
	return int(d.Hours() / 24)
}
