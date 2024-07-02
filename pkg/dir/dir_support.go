package dir

import (
	"errors"
	"github.com/rolandhe/smss/pkg/logger"
	"os"
	"strconv"
)

func EnsurePathExist(p string) error {
	dirInfo, err := os.Stat(p)
	if os.IsNotExist(err) {
		if err = os.Mkdir(p, os.ModePerm); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if !dirInfo.IsDir() {
		return errors.New(p + " exist,but is is not dir")
	}
	return nil
}

func PathExist(p string) (bool, error) {
	_, err := os.Stat(p)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func ParseNumber(s string) int64 {
	num, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		logger.Get().Infof("parse number for %s err: %v", s, err)
		return -1
	}
	return num
}

type BizError struct {
	message string
}

func (e *BizError) Error() string {
	return e.message
}

func IsBizErr(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*BizError)
	return ok
}
func NewBizError(msg string) error {
	return &BizError{
		message: msg,
	}
}
