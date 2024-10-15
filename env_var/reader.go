package env_var

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

type EnvVarReader struct {
	Logger *zap.Logger
}

func (reader EnvVarReader) MustFind(name string) string {
	varable, vaiableExists := os.LookupEnv(name)
	if !vaiableExists {
		reader.Logger.Error(fmt.Sprintf("%s is missing", name))
		panic(fmt.Errorf("%s is missing", name))
	}
	return varable
}
