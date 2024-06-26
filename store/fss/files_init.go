package fss

import (
	"errors"
	"github.com/rolandhe/smss/pkg"
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

func ensureMqDirectory(root string, mqList []*store.MqInfo) error {
	for _, info := range mqList {
		p := MqPath(root, info.Name)
		if err := pkg.EnsurePathExist(p); err != nil {
			return err
		}
	}
	return nil
}

func MqPath(root string, mqName string) string {
	h := hashString(mqName + "talk is cheap, show me your code")
	p1 := int(h % 100)
	h = hashString(mqName + "The world is beautiful")

	p2 := int(h % 100)

	return path.Join(root, strconv.Itoa(p1), strconv.Itoa(p2), mqName)
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
	for i := 0; i < 100; i++ {
		_, ok := recordMap[i]
		p := path.Join(root, strconv.Itoa(i))
		if !ok {
			if err = os.Mkdir(p, os.ModePerm); err != nil {
				return err
			}
			if next {
				for j := 0; j < 100; j++ {
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
	if num >= 0 && num <= 99 {
		return num
	}
	return -1
}
