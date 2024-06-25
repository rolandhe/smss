package backgroud

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/cmd/router"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/store"
	"log"
	"os"
	"strings"
	"time"
)

const (
	DeleteFileAfterStateChangeTimeout = int64(1000 * 60 * 5)
)

var cronIns *cron.Cron

func StartClearOldFiles(store store.Store, worker router.MessageWorking, delMqFileExecutor protocol.DelMqFileExecutor) {
	cronIns = cron.New()

	// 添加定时任务（每2小时执行一次）
	cronIns.AddFunc(fmt.Sprintf("@every %ds", conf.StoreClearInterval), func() {
		deleteOldFiles(store, worker, delMqFileExecutor)
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

func deleteOldFiles(fstore store.Store, worker router.MessageWorking, delMqFileExecutor protocol.DelMqFileExecutor) {
	traceId := fmt.Sprintf("delOldFile-%d", time.Now().UnixMilli())
	infoList, err := fstore.GetMqInfoReader().GetMQSimpleInfoList()
	if err != nil {
		log.Printf("deleteOldFiles err:%v\n", err)
	}

	locker := delMqFileExecutor.GetDeleteFileLocker()

	for _, info := range infoList {
		p := fstore.GetMqPath(info.Name)
		if store.MqStateNormal == info.State {
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
		if store.MqStateDeleted == info.State {
			if info.StateChange+DeleteFileAfterStateChangeTimeout <= time.Now().UnixMilli() {
				err = removeMq(worker, info.Name, traceId)
				log.Printf("tid=%s,deleteOldFiles to delete expired mq %s err:%v\n", traceId, info.Name, err)
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
		log.Printf("tid=%s,read dir:%s error:%v\n", traceId, p, err)
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
			log.Printf("tid=%s,file %s not valid log file\n", traceId, name)
			continue
		}
		num := pkg.ParseNumber(items[0])
		if num > maxId {
			maxId = num
		}

		info, err := entry.Info()
		if err != nil {
			log.Printf("tid=%s,read dir:%s/%s error:%v\n", traceId, p, entry.Name(), err)
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
			log.Printf("tid=%s,delete %s err:%v\n", traceId, dp, err)
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
