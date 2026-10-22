// SPDX-License-Identifier: AGPL-3.0-or-later

package version

import "strings"

const AgentName = "plural-star-cloud-node"

func AgentString(roles []string, pushBoxPub string) string {
	agent := AgentName + "/" + Version + " roles=" + strings.Join(roles, ",")
	if pushBoxPub != "" {
		agent += " pushbox=" + pushBoxPub
	}
	return agent
}

func ParseAgent(agent string) (roles []string, pushBoxPub string) {
	for _, field := range strings.Fields(agent) {
		if strings.HasPrefix(field, "roles=") {
			for _, r := range strings.Split(strings.TrimPrefix(field, "roles="), ",") {
				if r != "" {
					roles = append(roles, r)
				}
			}
		} else if strings.HasPrefix(field, "pushbox=") {
			pushBoxPub = strings.TrimPrefix(field, "pushbox=")
		}
	}
	if len(roles) == 0 {
		roles = []string{"relay"}
	}
	return roles, pushBoxPub
}
