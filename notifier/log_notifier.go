/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package notifier

import (
	"bytes"
	"fmt"
	log "github.com/cihub/seelog"
	"text/template"
)

type LogNotifier struct {
	TemplateFile string
	Groups       []string
	template     *template.Template
	Threshold    int
	groupMsgs    map[string]Message
}

func (logger *LogNotifier) NotifierName() string {
	return "log-notify"
}

func (logger *LogNotifier) Notify(msg Message) error {
	if logger.template == nil {
		template, err := template.ParseFiles(logger.TemplateFile)
		if err != nil {
			log.Critical("Cannot parse email template: %v", err)
			return err
		}
		logger.template = template
	}

	if logger.groupMsgs == nil {
		logger.groupMsgs = make(map[string]Message)
	}

	if len(logger.Groups) == 0 {
		clusterGroup := fmt.Sprintf("%s,%s", msg.Cluster, msg.Group)
		logger.groupMsgs[clusterGroup] = msg
	} else {
		for _, group := range logger.Groups {
			clusterGroup := fmt.Sprintf("%s,%s", msg.Cluster, msg.Group)
			if clusterGroup == group {
				logger.groupMsgs[clusterGroup] = msg
			}
		}
	}
	if len(logger.groupMsgs) > 0 {
		return logger.sendConsumerGroupStatusNotify()
	}
	return nil
}

func (logger *LogNotifier) Ignore(msg Message) bool {
	return int(msg.Status) < logger.Threshold
}

func (logger *LogNotifier) sendConsumerGroupStatusNotify() error {
	var bytesToSend bytes.Buffer

	msgs := make([]Message, len(logger.groupMsgs))
	i := 0
	for group, msg := range logger.groupMsgs {
		msgs[i] = msg
		delete(logger.groupMsgs, group)
		i++
	}

	err := logger.template.Execute(&bytesToSend, struct {
		Results []Message
	}{
		Results: msgs,
	})
	if err != nil {
		log.Error("Failed to assemble email:", err)
		return err
	}
	log.Info(bytesToSend.String())
	return nil
}
