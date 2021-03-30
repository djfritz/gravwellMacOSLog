/*************************************************************************
 * Copyright 2020 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"errors"

	"github.com/google/uuid"
	"github.com/gravwell/gravwell/v3/ingest/config"
)

type global struct {
	config.IngestConfig
	Tag_Name string
}

type cfgType struct {
	Global global
}

func GetConfig(path string) (*cfgType, error) {
	var c cfgType
	if err := config.LoadConfigFile(&c, path); err != nil {
		return nil, err
	}

	if err := verifyConfig(&c); err != nil {
		return nil, err
	}

	// Verify and set UUID
	if _, ok := c.Global.IngesterUUID(); !ok {
		id := uuid.New()
		if err := c.Global.SetIngesterUUID(id, path); err != nil {
			return nil, err
		}
		if id2, ok := c.Global.IngesterUUID(); !ok || id != id2 {
			return nil, errors.New("Failed to set a new ingester UUID")
		}
	}
	return &c, nil
}

func verifyConfig(c *cfgType) error {
	//verify the global parameters
	if err := c.Global.Verify(); err != nil {
		return err
	}

	if c.Global.Tag_Name == "" {
		c.Global.Tag_Name = "default"
	}

	return nil
}
