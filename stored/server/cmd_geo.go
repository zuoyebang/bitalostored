// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/geohash"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
)

type direction int

const (
	unsorted direction = iota
	asc
	desc
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.GEOADD:            {Sync: resp.IsWriteCmd(resp.GEOADD), Handler: geoaddCommand},
		resp.GEODIST:           {Sync: resp.IsWriteCmd(resp.GEODIST), Handler: geodistCommand},
		resp.GEOPOS:            {Sync: resp.IsWriteCmd(resp.GEOPOS), Handler: geoposCommand},
		resp.GEOHASH:           {Sync: resp.IsWriteCmd(resp.GEOHASH), Handler: geohashCommand},
		resp.GEORADIUS:         {Sync: resp.IsWriteCmd(resp.GEORADIUS), Handler: georadiusCommand},
		resp.GEORADIUSBYMEMBER: {Sync: resp.IsWriteCmd(resp.GEORADIUSBYMEMBER), Handler: georadiusbymemberCommand},
	})
}

func geoaddCommand(c *Client) error {
	args := c.Args
	if len(args) < 3 || len(args[1:])%3 != 0 {
		return resp.CmdParamsErr(resp.GEOADD)
	}

	key, args := args[0], args[1:]
	var params []btools.ScorePair
	for len(args) > 2 {
		rawLong, rawLat, name := args[0], args[1], args[2]
		args = args[3:]
		longitude, err := strconv.ParseFloat(string(rawLong), 64)
		if err != nil {
			return errors.New("ERR value is not a valid float")
		}
		latitude, err := strconv.ParseFloat(string(rawLat), 64)
		if err != nil {
			return errors.New("ERR value is not a valid float")
		}

		if latitude < -85.05112878 ||
			latitude > 85.05112878 ||
			longitude < -180 ||
			longitude > 180 {
			return errors.New(fmt.Sprintf("ERR invalid longitude,latitude pair %.6f,%.6f", longitude, latitude))
		}
		if score, err := geohash.EncodeWGS84(longitude, latitude); err != nil {
			return err
		} else {
			params = append(params, btools.ScorePair{Score: float64(score), Member: name})
		}
	}

	n, err := c.DB.ZAdd(key, c.KeyHash, params...)
	if err == nil {
		c.RespWriter.WriteInteger(n)
	}

	return err
}

func geodistCommand(c *Client) error {
	args := c.Args
	if len(args) < 3 {
		return resp.CmdParamsErr(resp.GEODIST)
	}
	if len(args) > 4 {
		return resp.ErrSyntax
	}
	key, from, to, args := args[0], args[1], args[2], args[3:]

	unit := "m"
	if len(args) > 0 {
		unit, args = string(args[0]), args[1:]
	}
	toMeter := parseUnit(unit)
	if toMeter == 0 {
		return errors.New("ERR unsupported unit provided. please use m, km, ft, mi")
	}

	fromD, errFrom := c.DB.ZScore(key, c.KeyHash, from)
	toD, errTo := c.DB.ZScore(key, c.KeyHash, to)
	if errFrom != nil || errTo != nil {
		c.RespWriter.WriteBulk(nil)
		return nil
	}
	dist := geohash.DistBetweenGeoHashWGS84(uint64(fromD), uint64(toD)) / toMeter
	c.RespWriter.WriteBulk([]byte(fmt.Sprintf("%.4f", dist)))
	return nil
}

func geoposCommand(c *Client) error {
	args := c.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.GEOPOS)
	}
	key, args := args[0], args[1:]
	arr := []interface{}{}
	for _, arg := range args {
		if score, err := c.DB.ZScore(key, c.KeyHash, arg); err != nil {
			arr = append(arr, nil)
		} else {
			long, lat := geohash.DecodeToLongLatWGS84(uint64(score))
			arr = append(arr, []interface{}{[]byte(strconv.FormatFloat(long, 'f', 17, 64)), []byte(strconv.FormatFloat(lat, 'f', 17, 64))})
		}
	}
	c.RespWriter.WriteArray(arr)
	return nil
}

func geohashCommand(c *Client) error {
	args := c.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.GEOHASH)
	}
	key, args := args[0], args[1:]
	arr := []interface{}{}
	for _, arg := range args {
		if score, err := c.DB.ZScore(key, c.KeyHash, arg); err != nil {
			arr = append(arr, nil)
		} else {
			longitude, latitude := geohash.DecodeToLongLatWGS84(uint64(score))
			code, _ := geohash.Encode(
				&geohash.Range{Max: 180, Min: -180},
				&geohash.Range{Max: 90, Min: -90},
				longitude,
				latitude,
				geohash.WGS84_GEO_STEP)
			arr = append(arr, geohash.EncodeToBase32(code.Bits))
		}
	}
	c.RespWriter.WriteArray(arr)
	return nil
}

func georadiusCommand(c *Client) error {
	args := c.Args
	if len(args) < 5 {
		return resp.CmdParamsErr(resp.GEORADIUS)
	}

	key := args[0]
	longitude, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return resp.CmdParamsErr(resp.GEORADIUS)
	}
	latitude, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return resp.CmdParamsErr(resp.GEORADIUS)
	}
	radius, err := strconv.ParseFloat(string(args[3]), 64)
	if err != nil || radius < 0 {
		return resp.CmdParamsErr(resp.GEORADIUS)
	}
	toMeter := parseUnit(string(args[4]))
	if toMeter == 0 {
		return resp.CmdParamsErr(resp.GEORADIUS)
	}
	args = args[5:]

	var (
		withDist  = false
		withCoord = false
		withHash  = false
		direction = unsorted
		count     = 0
	)
	for len(args) > 0 {
		arg := args[0]
		args = args[1:]
		switch strings.ToUpper(string(arg)) {
		case "WITHCOORD":
			withCoord = true
		case "WITHDIST":
			withDist = true
		case "WITHHASH":
			withHash = true
		case "ASC":
			direction = asc
		case "DESC":
			direction = desc
		case "COUNT":
			if len(args) == 0 {
				return resp.ErrSyntax
			}
			n, err := strconv.Atoi(string(args[0]))
			if err != nil {
				return resp.ErrValue
			}
			if n <= 0 {
				return errors.New("ERR COUNT must be > 0")
			}
			args = args[1:]
			count = n
		default:
			return resp.ErrSyntax
		}
	}

	radiusArea, err := geohash.GetAreasByRadiusWGS84(longitude, latitude, radius*toMeter)
	if err != nil {
		return err
	}

	matches, err := geoMembersOfAllNeighbors(c, key, radiusArea, longitude, latitude, radius*toMeter)
	if err != nil {
		return err
	}

	if direction != unsorted {
		sort.Slice(matches, func(i, j int) bool {
			if direction == desc {
				return matches[i].dist > matches[j].dist
			}
			return matches[i].dist < matches[j].dist
		})
	}

	if count > 0 && len(matches) > count {
		matches = matches[:count]
	}

	arr := []interface{}{}
	for _, member := range matches {
		if !withDist && !withCoord && !withHash {
			arr = append(arr, member.member)
			continue
		}
		item := []interface{}{member.member}
		if withDist {
			item = append(item, []byte(fmt.Sprintf("%.4f", member.dist/toMeter)))
		}
		if withHash {
			item = append(item, int64(member.score))
		}
		if withCoord {
			item = append(item, []interface{}{[]byte(strconv.FormatFloat(member.longitude, 'f', 17, 64)), []byte(strconv.FormatFloat(member.latitude, 'f', 17, 64))})
		}
		arr = append(arr, item)
	}
	c.RespWriter.WriteArray(arr)
	return nil
}

func georadiusbymemberCommand(c *Client) error {
	args := c.Args
	if len(args) < 4 {
		return resp.CmdParamsErr(resp.GEORADIUSBYMEMBER)
	}

	key := args[0]
	member := args[1]
	radius, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return resp.CmdParamsErr(resp.GEORADIUSBYMEMBER)
	}

	toMeter := parseUnit(string(args[3]))
	if toMeter == 0 {
		return resp.CmdParamsErr(resp.GEORADIUSBYMEMBER)
	}
	args = args[4:]

	var (
		withDist  = false
		withCoord = false
		withHash  = false
		direction = unsorted
		count     = 0
	)
	for len(args) > 0 {
		arg := args[0]
		args = args[1:]
		switch strings.ToUpper(string(arg)) {
		case "WITHCOORD":
			withCoord = true
		case "WITHDIST":
			withDist = true
		case "WITHHASH":
			withHash = true
		case "ASC":
			direction = asc
		case "DESC":
			direction = desc
		case "COUNT":
			if len(args) == 0 {
				return resp.ErrSyntax
			}
			n, err := strconv.Atoi(string(args[0]))
			if err != nil {
				return resp.ErrValue
			}
			if n <= 0 {
				return errors.New("ERR COUNT must be > 0")
			}
			args = args[1:]
			count = n
		default:
			return resp.ErrSyntax
		}
	}

	var longitude, latitude float64
	if score, err := c.DB.ZScore(key, c.KeyHash, member); err != nil {
		return errors.New("ERR could not decode requested zset member")
	} else {
		longitude, latitude = geohash.DecodeToLongLatWGS84(uint64(score))
	}
	radiusArea, err := geohash.GetAreasByRadiusWGS84(longitude, latitude, radius*toMeter)
	if err != nil {
		return err
	}

	matches, err := geoMembersOfAllNeighbors(c, key, radiusArea, longitude, latitude, radius*toMeter)
	if err != nil {
		return err
	}

	if direction != unsorted {
		sort.Slice(matches, func(i, j int) bool {
			if direction == desc {
				return matches[i].dist > matches[j].dist
			}
			return matches[i].dist < matches[j].dist
		})
	}

	if count > 0 && len(matches) > count {
		matches = matches[:count]
	}

	var arr []interface{}
	for _, member := range matches {
		if !withDist && !withCoord && !withHash {
			arr = append(arr, member.member)
			continue
		}
		item := []interface{}{member.member}
		if withDist {
			item = append(item, []byte(fmt.Sprintf("%.4f", member.dist/toMeter)))
		}
		if withHash {
			item = append(item, int64(member.score))
		}
		if withCoord {
			item = append(item, []interface{}{[]byte(strconv.FormatFloat(member.longitude, 'f', 17, 64)), []byte(strconv.FormatFloat(member.latitude, 'f', 17, 64))})
		}
		arr = append(arr, item)
	}
	c.RespWriter.WriteArray(arr)
	return nil
}

func parseUnit(u string) float64 {
	switch u {
	case "m":
		return 1
	case "km":
		return 1000
	case "mi":
		return 1609.34
	case "ft":
		return 0.3048
	default:
		return 0
	}
}

func geoMembersOfAllNeighbors(c *Client, set []byte, geoRadius *geohash.Radius, lon, lat, radius float64) ([]*geoPoints, error) {
	neighbors := [9]*geohash.HashBits{
		&geoRadius.Hash,
		&geoRadius.North,
		&geoRadius.South,
		&geoRadius.East,
		&geoRadius.West,
		&geoRadius.NorthEast,
		&geoRadius.NorthWest,
		&geoRadius.SouthEast,
		&geoRadius.SouthWest,
	}

	var lastProcessed int = 0
	plist := make([]*geoPoints, 0, 64)

	for i, area := range neighbors {
		if area.IsZero() {
			continue
		}
		if lastProcessed != 0 &&
			area.Bits == neighbors[lastProcessed].Bits &&
			area.Step == neighbors[lastProcessed].Step {
			continue
		}
		ps, err := membersOfGeoHashBox(c, set, lon, lat, radius, area)
		if err != nil {
			return nil, err
		} else {
			plist = append(plist, ps...)
		}
		lastProcessed = i
	}
	return plist, nil
}

func membersOfGeoHashBox(c *Client, zset []byte, longitude, latitude, radius float64, hash *geohash.HashBits) ([]*geoPoints, error) {
	points := make([]*geoPoints, 0, 32)
	min, max := scoresOfGeoHashBox(hash)
	vlist, err := c.DB.ZRangeByScoreGeneric(zset, c.KeyHash, float64(min), float64(max), false, false, 0, -1, false)
	if err != nil {
		return nil, err
	}

	for _, v := range vlist {
		x, y := geohash.DecodeToLongLatWGS84(uint64(v.Score))
		dist := geohash.GetDistance(x, y, longitude, latitude)
		if radius >= dist {
			p := &geoPoints{
				longitude: x,
				latitude:  y,
				dist:      dist,
				score:     float64(v.Score),
				member:    v.Member,
			}
			points = append(points, p)
		}
	}

	return points, nil
}

func scoresOfGeoHashBox(hash *geohash.HashBits) (min, max uint64) {
	min = hash.Bits << (geohash.WGS84_GEO_STEP*2 - hash.Step*2)
	bits := hash.Bits + 1
	max = bits << (geohash.WGS84_GEO_STEP*2 - hash.Step*2)
	return
}

type geoPoints struct {
	longitude float64
	latitude  float64
	dist      float64
	score     float64
	member    []byte
}
