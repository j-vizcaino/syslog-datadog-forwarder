package main

import (
	"context"
	"fmt"

	"github.com/DataDog/datadog-api-client-go/api/v1/datadog"
	"github.com/influxdata/go-syslog/v3/rfc3164"
)

func TransformMessages(ctx context.Context, in <- chan *rfc3164.SyslogMessage, publisher *Publisher) {
	for {
		select {
		case raw := <-in:
			tags := []interface{}{
				fmt.Sprintf("source:%s", *raw.Appname),
			}
			out := datadog.NewLogContent()
			out.SetHost(*raw.Hostname)
			out.SetMessage(*raw.Message)
			out.SetService(*raw.Appname)
			out.SetTags(tags)
			out.SetTimestamp(*raw.Timestamp)
			publisher.Publish(ctx, out)
		case <-ctx.Done():
			return
		}
	}
}