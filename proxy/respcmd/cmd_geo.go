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

package respcmd

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"

	"github.com/gomodule/redigo/redis"
)

func init() {
	resp.Register(resp.GEOADD, GeoAddCommand)
	resp.Register(resp.GEODIST, GeoDistCommand)
	resp.Register(resp.GEOHASH, GeoHashCommand)
	resp.Register(resp.GEOPOS, GeoPosCommand)
	resp.Register(resp.GEORADIUS, GeoRadiusCommand)
	resp.Register(resp.GEORADIUSBYMEMBER, GeoRadiusByMemberCommand)
}

func GeoAddCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 3 || len(args[1:])%3 != 0 {
		return resp.CmdParamsErr(resp.GEOADD)
	}

	key, args := args[0], args[1:]
	var params []interface{}
	params = append(params, key)
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
		params = append(params, longitude, latitude, name)
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	res, err := proxyClient.GeoAdd(s, params...)
	if s.TxCommandQueued {
		return s.SendTxQueued(err)
	} else {
		data, err2 := redis.Int64(res, err)
		if err2 != nil {
			return err2
		} else {
			s.RespWriter.WriteInteger(data)
			return nil
		}
	}
}

func GeoHashCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.GEOHASH)
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	data, err := proxyClient.GeoHash(s, args)
	if s.TxCommandQueued {
		return s.SendTxQueued(err)
	} else {
		if err != nil {
			return err
		} else {
			switch reply := data.(type) {
			case []interface{}:
				s.RespWriter.WriteArray(reply)
				return nil
			default:
				return errors.New("wrong data format for command GEOHASH")
			}
		}
	}
}

func GeoDistCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 && len(args) != 4 {
		return resp.CmdParamsErr(resp.GEODIST)
	}

	if len(args[3:]) > 0 {
		toMeter := parseUnit(string(args[3]))
		if toMeter == 0 {
			return errors.New("ERR unsupported unit provided. please use m, km, ft, mi")
		}
	}

	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	res, err := proxyClient.GeoDist(s, args)
	if s.TxCommandQueued {
		return s.SendTxQueued(err)
	} else {
		data, err2 := redis.Bytes(res, err)
		if err2 != nil {
			return err2
		} else {
			s.RespWriter.WriteBulk(data)
			return nil
		}
	}
}

func GeoPosCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.GEOPOS)
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	data, err := proxyClient.GeoPos(s, args)
	if s.TxCommandQueued {
		return s.SendTxQueued(err)
	} else {
		if err != nil {
			return err
		} else {
			switch reply := data.(type) {
			case []interface{}:
				s.RespWriter.WriteArray(reply)
				return nil
			default:
				return errors.New("wrong data format for command GEOPOS")
			}
		}
	}
}

func GeoRadiusCommand(s *resp.Session) error {
	args := s.Args
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
	var params []interface{}
	params = append(params, key, longitude, latitude, radius)
	for _, data := range args[4:] {
		params = append(params, unsafe2.String(data))
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		reply, err := proxyClient.GeoRadius(s, params...)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		}
		if err != nil {
			return err
		}
		switch reply := reply.(type) {
		case []interface{}:
			s.RespWriter.WriteArray(reply)
		case int64:
			s.RespWriter.WriteInteger(reply)
		case error:
			s.RespWriter.WriteError(reply)
		default:
			return fmt.Errorf("georadius response: unexpected type for %s, got type %T", resp.GEORADIUS, reply)
		}
	} else {
		return err
	}
	return nil
}

func GeoRadiusByMemberCommand(s *resp.Session) error {
	args := s.Args
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
	var params []interface{}
	params = append(params, key, member, radius)
	for _, data := range args[3:] {
		params = append(params, unsafe2.String(data))
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		reply, err := proxyClient.GeoRadiusByMember(s, params...)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		}
		if err != nil {
			return err
		}
		switch reply := reply.(type) {
		case []interface{}:
			s.RespWriter.WriteArray(reply)
		case int64:
			s.RespWriter.WriteInteger(reply)
		case error:
			s.RespWriter.WriteError(reply)
		default:
			return fmt.Errorf("georadius response: unexpected type for %s, got type %T", resp.GEORADIUS, reply)
		}
	} else {
		return err
	}
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
