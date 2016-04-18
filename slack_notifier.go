/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/cihub/seelog"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"
)

type SlackNotifier struct {
	app            *ApplicationContext
	refreshTicker  *time.Ticker
	quitChan       chan struct{}
	groupIds       map[string]map[string]slackEvent
	groupList      map[string]map[string]bool
	groupLock      sync.RWMutex
	resultsChannel chan *ConsumerGroupStatus
	slackMessage   *SlackMessage
	httpClient     *http.Client
}

type SlackMessage struct {
	Channel     string       `json:"channel"`
	Username    string       `json:"username"`
	IconUrl     string       `json:"icon_url"`
	IconEmoji   string       `json:"icon_emoji"`
	Text        string       `json:"text,omitempty"`
	Attachments []attachment `json:"attachments,omitempty"`
}

type attachment struct {
	Color    string   `json:"color"`
	Title    string   `json:"title"`
	Pretext  string   `json:"pretext"`
	Fallback string   `json:"fallback"`
	Text     string   `json:"text"`
	MrkdwnIn []string `json:"mrkdwn_in"`
}

type slackEvent struct {
	Id    string
	Start time.Time
}

func NewSlackNotifier(app *ApplicationContext) (*SlackNotifier, error) {
	// Helper functions for templates
	return &SlackNotifier{
		app:            app,
		quitChan:       make(chan struct{}),
		groupIds:       make(map[string]map[string]slackEvent),
		groupList:      make(map[string]map[string]bool),
		groupLock:      sync.RWMutex{},
		resultsChannel: make(chan *ConsumerGroupStatus),
		slackMessage: &SlackMessage{
			Channel:   app.Config.Slacknotifier.Channel,
			Username:  app.Config.Slacknotifier.Username,
			IconUrl:   "https://slack.com/img/icons/app-57.png",
			IconEmoji: ":ghost:",
		},
		httpClient: &http.Client{
			Timeout: time.Duration(app.Config.Slacknotifier.Timeout) * time.Second,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					KeepAlive: time.Duration(app.Config.Slacknotifier.Keepalive) * time.Second,
				}).Dial,
				Proxy: http.ProxyFromEnvironment,
			},
		},
	}, nil
}

func (slack *SlackNotifier) handleEvaluationResponse(result *ConsumerGroupStatus) {
	if int(result.Status) < slack.app.Config.Slacknotifier.PostThreshold {
		return
	}

	var emoji, color string
	switch result.Status {
	case StatusOK:
		emoji = ":white_check_mark:"
		color = "good"
	case StatusNotFound, StatusWarning:
		emoji = ":question:"
		color = "warning"
	default:
		emoji = ":x:"
		color = "danger"
	}

	title := "Burrow monitoring report"
	fallback := fmt.Sprintf("%s is %s", result.Group, result.Status)
	pretext := fmt.Sprintf("%s Group `%s` in Cluster `%s` is *%s*", emoji, result.Group, result.Cluster, result.Status)

	detailedBody := fmt.Sprintf("*Detail:* Total Partition = `%d` Fail Partition = `%d`\n",
		result.TotalPartitions, len(result.Partitions))

	for _, p := range result.Partitions {
		detailedBody += fmt.Sprintf("*%s* *[%s:%d]* (%d, %d) -> (%d, %d)\n",
			p.Status.String(), p.Topic, p.Partition, p.Start.Offset, p.Start.Lag, p.End.Offset, p.End.Lag)
	}

	a := attachment{
		Color:    color,
		Title:    title,
		Fallback: fallback,
		Pretext:  pretext,
		Text:     detailedBody,
		MrkdwnIn: []string{"text", "pretext"},
	}
	slack.slackMessage.Attachments = []attachment{a}

	slack.postToSlack()
}

func (notifier *SlackNotifier) refreshConsumerGroups() {
	notifier.groupLock.Lock()
	defer notifier.groupLock.Unlock()

	for cluster, _ := range notifier.app.Config.Kafka {
		clusterGroups, ok := notifier.groupList[cluster]
		if !ok {
			notifier.groupList[cluster] = make(map[string]bool)
			clusterGroups = notifier.groupList[cluster]
		}

		// Get a current list of consumer groups
		storageRequest := &RequestConsumerList{Result: make(chan []string), Cluster: cluster}
		notifier.app.Storage.requestChannel <- storageRequest
		consumerGroups := <-storageRequest.Result

		// Mark all existing groups false
		for consumerGroup := range notifier.groupList {
			clusterGroups[consumerGroup] = false
		}

		// Check for new groups, mark existing groups true
		for _, consumerGroup := range consumerGroups {
			// Don't bother adding groups in the blacklist
			if (notifier.app.Storage.groupBlacklist != nil) && notifier.app.Storage.groupBlacklist.MatchString(consumerGroup) {
				continue
			}

			if _, ok := clusterGroups[consumerGroup]; !ok {
				// Add new consumer group and start checking it
				log.Debugf("Start evaluating consumer group %s in cluster %s", consumerGroup, cluster)
				go notifier.startConsumerGroupEvaluator(consumerGroup, cluster)
			}
			clusterGroups[consumerGroup] = true
		}

		// Delete groups that are still false
		for consumerGroup := range clusterGroups {
			if !clusterGroups[consumerGroup] {
				log.Debugf("Remove evaluator for consumer group %s in cluster %s", consumerGroup, cluster)
				delete(clusterGroups, consumerGroup)
			}
		}
	}
}

func (notifier *SlackNotifier) startConsumerGroupEvaluator(group string, cluster string) {
	// Sleep for a random portion of the check interval
	time.Sleep(time.Duration(rand.Int63n(notifier.app.Config.Slacknotifier.Interval*1000)) * time.Millisecond)

	for {
		// Make sure this group still exists
		notifier.groupLock.RLock()
		if _, ok := notifier.groupList[cluster][group]; !ok {
			notifier.groupLock.RUnlock()
			log.Debugf("Stopping evaluator for consumer group %s in cluster %s", group, cluster)
			break
		}
		notifier.groupLock.RUnlock()

		// Send requests for group status - responses are handled by the main loop (for now)
		storageRequest := &RequestConsumerStatus{Result: notifier.resultsChannel, Cluster: cluster, Group: group}
		notifier.app.Storage.requestChannel <- storageRequest

		// Sleep for the check interval
		time.Sleep(time.Duration(notifier.app.Config.Slacknotifier.Interval) * time.Second)
	}
}

func (notifier *SlackNotifier) Start() {
	// Get a group list to start with (this will start the notifiers)
	notifier.refreshConsumerGroups()

	// Set a ticker to refresh the group list periodically
	notifier.refreshTicker = time.NewTicker(time.Duration(notifier.app.Config.Lagcheck.ZKGroupRefresh) * time.Second)

	// Main loop to handle refreshes and evaluation responses
	go func() {
	OUTERLOOP:
		for {
			select {
			case <-notifier.quitChan:
				break OUTERLOOP
			case <-notifier.refreshTicker.C:
				notifier.refreshConsumerGroups()
			case result := <-notifier.resultsChannel:
				go notifier.handleEvaluationResponse(result)
			}
		}
	}()
}

func (notifier *SlackNotifier) Stop() {
	if notifier.refreshTicker != nil {
		notifier.refreshTicker.Stop()
		notifier.groupLock.Lock()
		notifier.groupList = make(map[string]map[string]bool)
		notifier.groupLock.Unlock()
	}
	close(notifier.quitChan)
}

func (slack *SlackNotifier) postToSlack() bool {
	data, err := json.Marshal(slack.slackMessage)
	if err != nil {
		log.Errorf("Unable to marshal slack payload:", err)
		return false
	}
	log.Infof("struct = %+v, json = %s", slack.slackMessage, string(data))

	b := bytes.NewBuffer(data)
	req, err := http.NewRequest("POST", slack.app.Config.Slacknotifier.Url, b)
	req.Header.Set("Content-Type", "application/json")

	if res, err := slack.httpClient.Do(req); err != nil {
		log.Errorf("Unable to send data to slack:%+v", err)
		return false
	} else {
		defer res.Body.Close()
		statusCode := res.StatusCode
		if statusCode != 200 {
			body, _ := ioutil.ReadAll(res.Body)
			log.Errorf("Unable to notify slack:", string(body))
			return false
		} else {
			log.Debug("Slack notification sent")
			return true
		}
	}
}
