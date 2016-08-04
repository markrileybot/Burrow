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
	"errors"
	"fmt"
	log "github.com/cihub/seelog"
	"io/ioutil"
	"net/http"
	"net/url"
	"text/template"
	"strings"
)

type TwilioNotifier struct {
	Url           string
	To            []string
	From          string
	TemplateFile  string
	HttpClient    *http.Client
	Groups        []string
	Threshold     int
	Sid	      string
	Token	      string
	groupMsgs     map[string]Message
	template     *template.Template
}

func (twilio *TwilioNotifier) NotifierName() string {
	return "twilio-notify"
}

func (twilio *TwilioNotifier) Ignore(msg Message) bool {
	return int(msg.Status) < twilio.Threshold
}

func (twilio *TwilioNotifier) Notify(msg Message) error {
	if twilio.groupMsgs == nil {
		twilio.groupMsgs = make(map[string]Message)
	}

	if twilio.template == nil {
		template, err := template.ParseFiles(twilio.TemplateFile)
		if err != nil {
			log.Critical("Cannot parse twilio template: %v", err)
			return err
		}
		twilio.template = template
	}

	if len(twilio.Groups) == 0 {
		clusterGroup := fmt.Sprintf("%s,%s", msg.Cluster, msg.Group)
		twilio.groupMsgs[clusterGroup] = msg
	} else {
		for _, group := range twilio.Groups {
			clusterGroup := fmt.Sprintf("%s,%s", msg.Cluster, msg.Group)
			if clusterGroup == group {
				twilio.groupMsgs[clusterGroup] = msg
			}
		}
	}
	if len(twilio.groupMsgs) > 0 {
		return twilio.sendConsumerGroupStatusNotify()
	}
	return nil
}

func (twilio *TwilioNotifier) sendConsumerGroupStatusNotify() error {
	var buf bytes.Buffer
	msgs := make([]Message, len(twilio.groupMsgs))
	i := 0
	for group, msg := range twilio.groupMsgs {
		msgs[i] = msg
		delete(twilio.groupMsgs, group)
		i++
	}

	err := twilio.template.Execute(&buf, struct {
		From    string
		To      string
		Results []Message
	}{
		Results: msgs,
	})

	if err != nil {
		log.Error("Failed to assemble twilio message:", err)
		return err
	}

	body := buf.String()
	for _, to := range twilio.To {
		formData := strings.NewReader(url.Values{
			"To": {to},
			"From": {twilio.From},
			"Body": {body},
		}.Encode())

		if req, err := http.NewRequest("POST", twilio.Url, formData); err != nil {
			log.Errorf("Failed to create twilio request:%+v", err)
			return err
		} else {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.SetBasicAuth(twilio.Sid, twilio.Token)

			if rep, err := twilio.HttpClient.Do(req); err != nil {
				log.Errorf("Unable to send data to twilio:%+v", err)
				return err
			} else {
				statusCode := rep.StatusCode
				ret := ""
				if statusCode >= 400 {
					body, _ := ioutil.ReadAll(rep.Body)
					log.Errorf("Unable to notify twilio (status=%d): %s", statusCode, string(body))
					ret = "Send to twilio failed"
				} else {
					log.Debug("Twilio notification sent")
				}
				rep.Body.Close()

				if ret != "" {
					return errors.New(ret)
				}
			}
		}
	}

	return nil
}
