package logger

const DefaultSampleCount = 100

type SampleLoggerSupport interface {
	CanLogger() bool
}

func NewSampleLoggerSupport(sampleCount int64) SampleLoggerSupport {
	if sampleCount == 0 {
		sampleCount = DefaultSampleCount
	}
	return &sampleLogger{
		sampleCount: sampleCount,
	}
}

type sampleLogger struct {
	count       int64
	sampleCount int64
}

func (sl *sampleLogger) CanLogger() bool {
	can := sl.count%sl.sampleCount == 0
	sl.count++
	return can
}
