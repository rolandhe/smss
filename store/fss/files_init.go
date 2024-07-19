package fss

import (
	"errors"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/store"
	"hash/fnv"
	"os"
	"path"
	"strconv"
)

func ensureStoreDirectory(root string) error {
	info, err := os.Stat(root)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(root, os.ModePerm); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if !info.IsDir() {
		return errors.New(root + " is not dir")
	}

	return checkAndCreateInitDir(root, true)
}

func ensureTopicDirectory(root string, topicInfoList []*store.TopicInfo) error {
	for _, info := range topicInfoList {
		p := TopicPath(root, info.Name)
		if err := dir.EnsurePathExist(p); err != nil {
			return err
		}
	}
	return nil
}

func TopicPath(root string, topicName string) string {
	h := hashString(topicName + "talk is cheap, show me your code")
	p1 := int(h % conf.TopicFolderCount)
	h = hashString(topicName + "The world is beautiful")

	p2 := int(h % conf.TopicFolderCount)

	return path.Join(root, strconv.Itoa(p1), strconv.Itoa(p2), topicName)
}

func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func readParentDir(root string) (map[int]struct{}, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}

	recordMap := map[int]struct{}{}
	for _, entry := range entries {
		num := validParentDir(entry.Name())
		if -1 == num {
			continue
		}
		if !entry.IsDir() {
			return nil, errors.New(entry.Name() + " must be dir")
		}
		recordMap[num] = struct{}{}
	}
	return recordMap, nil
}

func checkAndCreateInitDir(root string, next bool) error {
	recordMap, err := readParentDir(root)
	if err != nil {
		return err
	}

	var existParent []string
	tfCountLimit := int(conf.TopicFolderCount)
	for i := 0; i < tfCountLimit; i++ {
		_, ok := recordMap[i]
		p := path.Join(root, strconv.Itoa(i))
		if !ok {
			if err = os.Mkdir(p, os.ModePerm); err != nil {
				return err
			}
			if next {
				for j := 0; j < tfCountLimit; j++ {
					if err = os.Mkdir(path.Join(p, strconv.Itoa(j)), os.ModePerm); err != nil {
						return err
					}
				}
			}
			continue
		}
		if next {
			existParent = append(existParent, p)
		}
	}

	for _, p := range existParent {
		if err = checkAndCreateInitDir(p, false); err != nil {
			return err
		}
	}

	return nil
}

func validParentDir(s string) int {
	num, err := strconv.Atoi(s)
	if err != nil {
		return -1
	}
	tfCountLimit := int(conf.TopicFolderCount)
	if num >= 0 && num < tfCountLimit {
		return num
	}
	return -1
}
