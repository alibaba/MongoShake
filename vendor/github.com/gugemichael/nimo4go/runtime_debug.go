// +build debug

package nimo

func AssertTrue(assert bool, message string) {
	if !assert {
		panic(message)
	}
}

func Assert(message string) {
	panic(message)
}
