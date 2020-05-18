package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/DataDog/datadog-api-client-go/api/v1/datadog"
)

type Publisher struct {
	apiKey string
	targetURL string
	client http.Client
	input chan *datadog.LogContent
}

func NewPublisher(ddApiKey string, intakeURL string) *Publisher {
	return &Publisher{
		apiKey:    ddApiKey,
		targetURL: intakeURL,
		input:     make(chan *datadog.LogContent, 128),
	}
}

func (p *Publisher) Publish(ctx context.Context, msg *datadog.LogContent) {
	select {
	case p.input <- msg:
	case <-ctx.Done():
	}
}

func (p *Publisher) Run(ctx context.Context) {
	for {
		select {
		case msg := <-p.input:
			p.publishNow(ctx, msg)
		case <- ctx.Done():
			return
		}
	}
}

func (p *Publisher) publishNow(ctx context.Context, msg *datadog.LogContent) {
	payload := bytes.Buffer{}
	encoder := json.NewEncoder(&payload)
	err := encoder.Encode(msg)
	if err != nil {
		fmt.Print(err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", p.targetURL, &payload)
	if err != nil {
		fmt.Print(err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DD-API-KEY", p.apiKey)

	res, err := p.client.Do(req)
	if err != nil {
		fmt.Print(err)
		return
	}

	if res.StatusCode != http.StatusOK {
		fmt.Print(res)
		return
	}
}