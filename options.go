package quasar

// TODO: rename? naming collides
type GetOption func(*getOptions)

type getOptions struct{}

func WaitForUID(uid int) GetOption {
	return func(opt *getOptions) {
		panic("not implemented")
	}
}
