package uglymodel

import (
	"crypto/md5"
	"fmt"
)

func GenID(uglyGroup, uglyId string) string {
	baseStr := fmt.Sprintf("%s:%s", uglyGroup, uglyId)
	return fmt.Sprintf("%x", md5.Sum([]byte(baseStr)))
}
