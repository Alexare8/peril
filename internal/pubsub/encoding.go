package pubsub

import (
	"bytes"
	"encoding/gob"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func encode(log routing.GameLog) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(log)
	return buffer.Bytes(), err
}

func decode(data []byte) (routing.GameLog, error) {
	reader := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(reader)
	var log routing.GameLog
	err := decoder.Decode(&log)
	return log, err
}
