package conf

import (
	"bufio"
	"strings"
	"strconv"
	"fmt"
	"os"
	"mongoshake/common"
)

// read the given file and parse the fcv do comparison
func CheckFcv(file string, fcv int) (int, error) {
	// read line by line and parse the version

	f, err := os.Open(file)
	if err != nil {
		return -1, err
	}

	scanner := bufio.NewScanner(f)
	versionName := "conf.version"
	version := 0
	for scanner.Scan() {
		field := strings.Split(scanner.Text(), "=")
		if len(field) >= 2 && strings.HasPrefix(field[0], versionName) {
			if value, err := strconv.Atoi(strings.Trim(field[1], " ")); err != nil {
				return 0, fmt.Errorf("illegal value[%v]", field[1])
			} else {
				version = value
				break
			}
		}
	}

	if version < fcv {
		return version, fmt.Errorf("current required configuration version[%v] > input[%v], please upgrade MongoShake to version >= %v",
			fcv, version, utils.LowestConfigurationVersion[fcv])
	}
	return version, nil
}