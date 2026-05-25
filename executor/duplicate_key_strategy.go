package executor

import (
	"fmt"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

func handleDupKeyOnInsert(conn *utils.MongoCommunityConn, database, collection string,
	oplogs []*OplogRecord, err error, logPrefix string) error {

	if !utils.DuplicateKey(err) {
		return err
	}

	switch conf.Options.IncrSyncExecutorDupKeyStrategy {
	case "", utils.VarIncrSyncExecutorDupKeyStrategyIgnore:
		RecordDuplicatedOplog(conn, collection, oplogs)
		l.Logger.Infof("%s duplicated oplogs ignored, ns[%s.%s], err[%v]",
			logPrefix, database, collection, err)
		return nil
	case utils.VarIncrSyncExecutorDupKeyStrategySkip:
		if skip, indexName := shouldSkipDupKeyOnInsert(database, collection, err); skip {
			RecordDuplicatedOplog(conn, collection, oplogs)
			l.Logger.Warnf("%s duplicated oplogs skipped, ns[%s.%s], index[%s], err[%v]",
				logPrefix, database, collection, indexName, err)
			return nil
		}
		return err
	case utils.VarIncrSyncExecutorDupKeyStrategyError,
		utils.VarIncrSyncExecutorDupKeyStrategyDeleteAndRetry:
		return err
	default:
		return fmt.Errorf("unknown dup key strategy[%s]: %w", conf.Options.IncrSyncExecutorDupKeyStrategy, err)
	}
}
