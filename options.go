package quasar

type LoadOption func(*loadOptions)

func getLoadOptions(opts []LoadOption) loadOptions {
	var cfg loadOptions
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

type loadOptions struct {
	waitFor int
}

func WaitForUID(uid int) LoadOption {
	return func(opt *loadOptions) {
		opt.waitFor = uid
	}
}
