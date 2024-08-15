package tm

import "time"

func ToDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
}

func NowDate() time.Time {
	return ToDate(time.Now())
}

func DiffDays(d1, d2 time.Time) int {
	d := d1.Sub(d2)
	return int(d.Hours() / 24)
}
