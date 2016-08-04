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
	log "github.com/cihub/seelog"
	"./notifier"
	"./protocol"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type NotifyCenter struct {
	app            *ApplicationContext
	interval       int64
	notifiers      []notifier.Notifier
	refreshTicker  *time.Ticker
	quitChan       chan struct{}
	groupList      map[string]map[string]int
	groupLock      sync.RWMutex
	resultsChannel chan *protocol.ConsumerGroupStatus
	groupStatus    map[string]map[string]protocol.StatusConstant
}

func LoadNotifiers(app *ApplicationContext) error {
	notifiers := []notifier.Notifier{}
	if app.Config.Httpnotifier.Url != "" {
		if httpNotifier, err := NewHttpNotifier(app); err == nil {
			notifiers = append(notifiers, httpNotifier)
		}
	}
	if len(app.Config.Emailnotifier) > 0 {
		if emailNotifiers, err := NewEmailNotifier(app); err == nil {
			for _, emailer := range emailNotifiers {
				notifiers = append(notifiers, emailer)
			}
		}
	}
	if app.Config.Slacknotifier.Url != "" {
		if slackNotifier, err := NewSlackNotifier(app); err == nil {
			notifiers = append(notifiers, slackNotifier)
		}
	}

	if app.Config.Lognotifier.Template != "" {
		if logNotifier, err := NewLogNotifier(app); err == nil {
			notifiers = append(notifiers, logNotifier)
		}
	}

	if app.Config.Twilionotifier.Url != "" {
		if twilioNotifier, err := NewTwilioNotifier(app); err == nil {
			notifiers = append(notifiers, twilioNotifier)
		}
	}

	nc := &NotifyCenter{
		app:            app,
		notifiers:      notifiers,
		interval:       app.Config.Notify.Interval,
		quitChan:       make(chan struct{}),
		groupList:      make(map[string]map[string]int),
		groupLock:      sync.RWMutex{},
		resultsChannel: make(chan *protocol.ConsumerGroupStatus),
		groupStatus:    make(map[string]map[string]protocol.StatusConstant),
	}

	app.NotifyCenter = nc
	return nil
}

func StartNotifiers(app *ApplicationContext) {
	nc := app.NotifyCenter
	// Do not proceed until we get the Zookeeper lock
	err := app.NotifierLock.Lock()
	if err != nil {
		log.Criticalf("Cannot get ZK nc lock: %v", err)
		os.Exit(1)
	}
	log.Info("Acquired Zookeeper notify lock")
	// Get a group list to start with (this will start the ncs)
	nc.refreshConsumerGroups()

	// Set a ticker to refresh the group list periodically
	nc.refreshTicker = time.NewTicker(time.Duration(nc.app.Config.Lagcheck.ZKGroupRefresh) * time.Second)

	// Main loop to handle refreshes and evaluation responses
OUTERLOOP:
	for {
		select {
		case <-nc.quitChan:
			break OUTERLOOP
		case <-nc.refreshTicker.C:
			nc.refreshConsumerGroups()
		case result := <-nc.resultsChannel:
			nc.handleEvaluationResponse(result)
		}
	}
}

func StopNotifiers(app *ApplicationContext) {
	// Ignore errors on unlock - we're quitting anyways, and it might not be locked
	app.NotifierLock.Unlock()
	nc := app.NotifyCenter
	if nc.refreshTicker != nil {
		nc.refreshTicker.Stop()
		nc.groupLock.Lock()
		nc.groupList = make(map[string]map[string]int)
		nc.groupLock.Unlock()
	}
	close(nc.quitChan)
	// TODO stop all ncs
}

func (nc *NotifyCenter) handleEvaluationResponse(result *protocol.ConsumerGroupStatus) {
	clusterStatuses, ok := nc.groupStatus[result.Cluster]
	if !ok {
		clusterStatuses = make(map[string]protocol.StatusConstant)
		nc.groupStatus[result.Cluster] = make(map[string]protocol.StatusConstant)
	}

	statusChanged := false
	groupStatus, ok := clusterStatuses[result.Group]
	if !ok {
		statusChanged = result.Status != protocol.StatusOK
	} else {
		statusChanged = result.Status != groupStatus
	}
	clusterStatuses[result.Group] = result.Status

	if statusChanged {
		msg := notifier.Message(*result)
		log.Infof("%s/%s status changed from %s to %s", result.Cluster, result.Group, groupStatus, result.Status)
		go nc.dispatchNotifications(msg)
	}
}

func (nc *NotifyCenter) dispatchNotifications(msg notifier.Message) {
	for _, notifier := range nc.notifiers {
		if !notifier.Ignore(msg) {
			notifier.Notify(msg)
		}
	}
}

func (nc *NotifyCenter) refreshConsumerGroups() {
	log.Info("refreshConsumerGroups")
	nc.groupLock.Lock()
	defer nc.groupLock.Unlock()

	for cluster, _ := range nc.app.Config.Kafka {
		clusterGroups, ok := nc.groupList[cluster]
		if !ok {
			nc.groupList[cluster] = make(map[string]int)
			clusterGroups = nc.groupList[cluster]
		}

		// Get a current list of consumer groups
		storageRequest := &RequestConsumerList{Result: make(chan []string), Cluster: cluster}
		nc.app.Storage.requestChannel <- storageRequest
		consumerGroups := <-storageRequest.Result

		// Check for new groups, mark existing groups true
		for _, consumerGroup := range consumerGroups {
			// Don't bother adding groups in the blacklist/whitelist
			if (!nc.app.Storage.AcceptConsumerGroup(consumerGroup)) {
				continue
			}

			var id int
			if id, ok = clusterGroups[consumerGroup]; !ok {
				// Add new consumer group and start checking it
				id = rand.Int()
				if id < 0 {
					id = -id
				} else if id == 0 {
					id = 1
				}
				log.Infof("Start evaluating consumer group %s in cluster %s", consumerGroup, cluster)
				go nc.startConsumerGroupEvaluator(consumerGroup, cluster, id)
			}
			clusterGroups[consumerGroup] = id
		}

		// Delete groups that are false
		for consumerGroup := range clusterGroups {
			if clusterGroups[consumerGroup] == 0 {
				log.Infof("Remove evaluator for consumer group %s in cluster %s", consumerGroup, cluster)
				delete(clusterGroups, consumerGroup)
			}
		}
	}
}

func (nc *NotifyCenter) startConsumerGroupEvaluator(group string, cluster string, id int) {
	// Sleep for a random portion of the check interval
	time.Sleep(time.Duration(rand.Int63n(nc.interval*1000)) * time.Millisecond)

	for {
		// Make sure this group still exists
		nc.groupLock.RLock()
		if cid, ok := nc.groupList[cluster][group]; !ok {
			nc.groupLock.RUnlock()
			log.Infof("Stopping evaluator for consumer group %s in cluster %s", group, cluster)
			break
		} else if id != cid {
			nc.groupLock.RUnlock()
			log.Infof("Stopping evaluator for consumer group %s in cluster %s as a different on exists!",
				group, cluster)
			break
		}
		nc.groupLock.RUnlock()

		// Send requests for group status - responses are handled by the main loop (for now)
		storageRequest := &RequestConsumerStatus{Result: nc.resultsChannel, Cluster: cluster, Group: group}
		nc.app.Storage.requestChannel <- storageRequest

		// Sleep for the check interval
		time.Sleep(time.Duration(nc.interval) * time.Second)
	}
}

func NewEmailNotifier(app *ApplicationContext) ([]*notifier.EmailNotifier, error) {
	log.Info("Start email notify")
	emailers := []*notifier.EmailNotifier{}
	for to, cfg := range app.Config.Emailnotifier {
		emailer := &notifier.EmailNotifier{
			Threshold:    cfg.Threshold,
			TemplateFile: app.Config.Smtp.Template,
			Server:       app.Config.Smtp.Server,
			Port:         app.Config.Smtp.Port,
			Username:     app.Config.Smtp.Username,
			Password:     app.Config.Smtp.Password,
			AuthType:     app.Config.Smtp.AuthType,
			Interval:     cfg.Interval,
			From:         app.Config.Smtp.From,
			To:           to,
			Groups:       cfg.Groups,
		}
		emailers = append(emailers, emailer)
	}

	return emailers, nil
}

func NewHttpNotifier(app *ApplicationContext) (*notifier.HttpNotifier, error) {
	httpConfig := app.Config.Httpnotifier

	// Parse the extra parameters for the templates
	extras := make(map[string]string)
	for _, extra := range httpConfig.Extras {
		parts := strings.Split(extra, "=")
		extras[parts[0]] = parts[1]
	}

	return &notifier.HttpNotifier{
		Url:                httpConfig.Url,
		Threshold:          httpConfig.PostThreshold,
		SendDelete:         httpConfig.SendDelete,
		TemplatePostFile:   httpConfig.TemplatePost,
		TemplateDeleteFile: httpConfig.TemplateDelete,
		Extras:             extras,
		HttpClient: &http.Client{
			Timeout: time.Duration(httpConfig.Timeout) * time.Second,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					KeepAlive: time.Duration(httpConfig.Keepalive) * time.Second,
				}).Dial,
				Proxy: http.ProxyFromEnvironment,
			},
		},
	}, nil
}

func NewSlackNotifier(app *ApplicationContext) (*notifier.SlackNotifier, error) {
	log.Info("Start Slack Notify")

	return &notifier.SlackNotifier{
		Url:       app.Config.Slacknotifier.Url,
		Groups:    app.Config.Slacknotifier.Groups,
		Threshold: app.Config.Slacknotifier.Threshold,
		Channel:   app.Config.Slacknotifier.Channel,
		Username:  app.Config.Slacknotifier.Username,
		IconUrl:   app.Config.Slacknotifier.IconUrl,
		IconEmoji: app.Config.Slacknotifier.IconEmoji,
		HttpClient: &http.Client{
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

func NewTwilioNotifier(app *ApplicationContext) (*notifier.TwilioNotifier, error) {
	log.Info("Start Twilio Notify")

	return &notifier.TwilioNotifier{
		Url:          app.Config.Twilionotifier.Url,
		Groups:       app.Config.Twilionotifier.Groups,
		Threshold:    app.Config.Twilionotifier.Threshold,
		Token:        app.Config.Twilionotifier.Token,
		Sid:          app.Config.Twilionotifier.Sid,
		To:           app.Config.Twilionotifier.To,
		From:         app.Config.Twilionotifier.From,
		TemplateFile: app.Config.Twilionotifier.Template,
		HttpClient: &http.Client{
			Timeout: time.Duration(app.Config.Twilionotifier.Timeout) * time.Second,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					KeepAlive: time.Duration(app.Config.Twilionotifier.Keepalive) * time.Second,
				}).Dial,
				Proxy: http.ProxyFromEnvironment,
			},
		},
	}, nil
}

func NewLogNotifier(app *ApplicationContext) (*notifier.LogNotifier, error) {
	log.Info("Start Log Notify")

	return &notifier.LogNotifier{
		Groups:       app.Config.Lognotifier.Groups,
		Threshold:    app.Config.Lognotifier.Threshold,
		TemplateFile: app.Config.Lognotifier.Template,
	}, nil
}
