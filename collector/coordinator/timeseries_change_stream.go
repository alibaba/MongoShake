package coordinator

import (
	"fmt"
	"strings"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

// mostTimeSeriesNamesShown caps how many namespaces the warning lists, so a source with
// hundreds of monthly-partitioned time-series collections does not bury the log line.
const mostTimeSeriesNamesShown = 10

// timeSeriesUnsupportedByChangeStream returns the in-scope time-series collections on
// these sources, or nil when the incremental fetch method is not change_stream.
//
// Change streams never surface time-series writes, on any MongoDB version. Measured
// against a replica set where one batch of measurements is written to a time-series
// collection and one to an ordinary collection in the same window: the oplog holds every
// bucket insert and update, a cluster-wide $changeStream returns only the ordinary
// collection's events, adding showExpandedEvents contributes nothing but the create DDL,
// and watching the logical collection directly is rejected by the server with "Cannot run
// aggregation on timeseries". Identical on 7.0.37, 7.0.43, 8.0.4 and 8.2.12. See
// https://www.mongodb.com/docs/manual/core/timeseries/timeseries-limitations/
//
// A source that cannot be reached or queried is skipped rather than failed: only a
// warning is at stake here, and the startup checks that do gate the run have already
// validated the connections.
func timeSeriesUnsupportedByChangeStream(sources []*utils.MongoSource) []string {
	if conf.Options.IncrSyncMongoFetchMethod != utils.VarIncrSyncMongoFetchMethodChangeStream {
		return nil
	}

	namespaceFilter := filter.NewNamespaceFilter(conf.Options.FilterNamespaceWhite,
		conf.Options.FilterNamespaceBlack)
	// FilterNs reports true for a namespace that should be dropped; ListTimeSeriesNamespaces
	// wants the opposite convention, like GetDbNamespace.
	inScope := func(ns string) bool { return !namespaceFilter.FilterNs(ns) }

	var found []string
	seen := make(map[string]bool)
	for _, src := range sources {
		conn, err := utils.NewMongoCommunityConn(src.URL, conf.Options.MongoConnectMode, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault,
			conf.Options.MongoSslRootCaFile)
		if conn == nil || err != nil {
			l.Logger.Warnf("cannot check source[%s] for time-series collections: %v", src, err)
			continue
		}
		namespaces, err := utils.ListTimeSeriesNamespaces(conn, inScope)
		conn.Close()
		if err != nil {
			l.Logger.Warnf("cannot list time-series collections on source[%s]: %v", src, err)
			continue
		}
		for _, ns := range namespaces {
			if !seen[ns] {
				seen[ns] = true
				found = append(found, ns)
			}
		}
	}
	return found
}

// warnChangeStreamCannotSyncTimeSeries tells the operator that the incremental phase will
// silently skip some of the collections they asked for.
//
// Without it the run looks healthy: the change stream keeps returning resume tokens, so
// the checkpoint advances on schedule, and the only symptom is write_success staying at 0
// alongside a filter count that accounts for everything fetched. There is no error to
// find, which is what makes the omission worth a log line of its own.
func warnChangeStreamCannotSyncTimeSeries(sources []*utils.MongoSource) {
	namespaces := timeSeriesUnsupportedByChangeStream(sources)
	if len(namespaces) == 0 {
		return
	}

	shown, suffix := namespaces, ""
	if len(shown) > mostTimeSeriesNamesShown {
		shown = shown[:mostTimeSeriesNamesShown]
		suffix = fmt.Sprintf(", ... and %d more", len(namespaces)-mostTimeSeriesNamesShown)
	}
	l.Logger.Warnf("incr_sync.mongo_fetch_method is change_stream, which cannot sync time-series "+
		"collections: MongoDB change streams do not surface time-series writes on any version, the "+
		"bucket inserts and updates only ever reach the oplog. These %d in-scope collection(s) will "+
		"NOT be synced incrementally: %s%s. The run will still look healthy - the checkpoint keeps "+
		"advancing while write_success stays 0. Set incr_sync.mongo_fetch_method = oplog to sync "+
		"them. See https://www.mongodb.com/docs/manual/core/timeseries/timeseries-limitations/",
		len(namespaces), strings.Join(shown, ", "), suffix)
}
