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

package cmd_test

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type TestData struct {
	Command       string
	Args          []interface{}
	ResultType    string
	ExpectedValue string
	LineNumber    int
}

func convertArgToProperType(arg string) interface{} {
	if intValue, err := strconv.ParseInt(arg, 10, 64); err == nil {
		return intValue
	}

	if floatValue, err := strconv.ParseFloat(arg, 64); err == nil {
		return floatValue
	}

	return arg
}

func parseTestData(filePath string) ([]TestData, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var testData []TestData
	scanner := bufio.NewScanner(file)

	lineNum := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lineNum++

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) < 4 {
			continue
		}

		command := parts[0]
		resultType := parts[len(parts)-2]
		expectedValue := parts[len(parts)-1]

		args := make([]interface{}, 0)
		for i := 1; i < len(parts)-2; i++ {
			arg := parts[i]

			arg = processPlaceholder(arg)

			convertedArg := convertArgToProperType(arg)
			args = append(args, convertedArg)
		}

		testData = append(testData, TestData{
			Command:       command,
			Args:          args,
			ResultType:    resultType,
			ExpectedValue: expectedValue,
			LineNumber:    lineNum,
		})
	}

	return testData, scanner.Err()
}

func processPlaceholder(value string) string {
	switch value {
	case "{{future_time}}":
		return strconv.FormatInt(time.Now().Unix()+100, 10)
	default:
		return value
	}
}

func compareArrays(t *testing.T, actual, expected interface{}) bool {
	actualSlice, actualIsSlice := actual.([]interface{})
	expectedSlice, expectedIsSlice := expected.([]interface{})

	if actualIsSlice && expectedIsSlice {
		if len(actualSlice) != len(expectedSlice) {
			return false
		}
		for i := 0; i < len(actualSlice); i++ {
			if !compareArrays(t, actualSlice[i], expectedSlice[i]) {
				return false
			}
		}
		return true
	}

	if !actualIsSlice && !expectedIsSlice {
		actualStr := interfaceToString(actual)
		expectedStr := interfaceToString(expected)
		return actualStr == expectedStr
	}

	return false
}

func interfaceToString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'g', -1, 64)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", val)
	}
}

func parseNestedArrayString(input string) (interface{}, error) {
	if input == "" {
		return []interface{}{}, nil
	}

	input = strings.TrimSpace(input)
	if input == "[]" {
		return []interface{}{}, nil
	}

	if strings.HasPrefix(input, "[") && strings.HasSuffix(input, "]") && strings.Contains(input, "[") && strings.Count(input, "[") > 1 {
		return parseComplexNestedArray(input)
	}

	return parseSimpleArray(input), nil
}

func parseSimpleArray(input string) []interface{} {
	input = strings.TrimPrefix(input, "[")
	input = strings.TrimSuffix(input, "]")

	if input == "" {
		return []interface{}{}
	}

	fields := strings.Fields(input)
	result := make([]interface{}, len(fields))
	for i, field := range fields {
		if field == "<nil>" {
			result[i] = nil
		} else {
			result[i] = field
		}
	}
	return result
}

func parseComplexNestedArray(input string) (interface{}, error) {
	input = strings.TrimSpace(input)
	if input == "[]" {
		return []interface{}{}, nil
	}

	input = strings.TrimPrefix(input, "[")
	input = strings.TrimSuffix(input, "]")

	var result []interface{}

	level := 0
	start := 0

	for i, char := range input {
		switch char {
		case '[':
			if level == 0 {
				if i > start {
					subStr := strings.TrimSpace(input[start:i])
					if subStr != "" {
						for _, elem := range strings.Fields(subStr) {
							if elem == "<nil>" {
								result = append(result, nil)
							} else {
								result = append(result, elem)
							}
						}
					}
					start = i
				}
			}
			level++
		case ']':
			level--
			if level == 0 {
				subArrayStr := strings.TrimSpace(input[start : i+1])
				subResult, err := parseNestedArrayString(subArrayStr)
				if err != nil {
					return nil, err
				}
				result = append(result, subResult)
				start = i + 1
			}
		}
	}

	remaining := strings.TrimSpace(input[start:])
	if remaining != "" {
		for _, elem := range strings.Fields(remaining) {
			if elem == "<nil>" {
				result = append(result, nil)
			} else {
				result = append(result, elem)
			}
		}
	}

	return result, nil
}

func TestAllCommandsFromData(t *testing.T) {
	closeServer, err := startServer(testDBConf, testDBPort)
	require.NoError(t, err)
	defer closeServer()

	time.Sleep(100 * time.Millisecond)

	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	testData, err := parseTestData("./testdata/unified_cmd_data")
	require.NoError(t, err)

	for i, td := range testData {
		t.Run(td.Command+strconv.Itoa(i), func(t *testing.T) {
			var reply interface{}
			var err error
			maxRetries := 2
			for attempt := 0; attempt <= maxRetries; attempt++ {
				reply, err = c.Do(strings.ToUpper(td.Command), td.Args...)
				if err == nil {
					break // Success, exit retry loop
				}

				errStr := err.Error()
				if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "connection") || strings.Contains(errStr, "closed network") {
					if attempt < maxRetries {
						c.Close()
						c = getTestConnWithAddr(testDBPort)
						time.Sleep(10 * time.Millisecond)
						continue
					}
				}

				break
			}

			switch td.ResultType {
			case "simple":
				if err != nil {
					t.Errorf("Unexpected error: %v. Line: %d, Command: %s, Args: %v", err, td.LineNumber, td.Command, td.Args)
					return
				}
			case "error":
				if err == nil {
					t.Errorf("Expected error but got success. Line: %d, Command: %s, Args: %v", td.LineNumber, td.Command, td.Args)
				}
				return
			case "random":
				if err != nil {
					t.Errorf("Unexpected error: %v. Line: %d, Command: %s, Args: %v", err, td.LineNumber, td.Command, td.Args)
					return
				}

				if td.Command == "SRANDMEMBER" {
					hasCount := false
					for _, arg := range td.Args {
						switch v := arg.(type) {
						case string:
							if _, err := strconv.Atoi(v); err == nil {
								hasCount = true
								break
							}
						case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
							hasCount = true
							break
						}
					}

					if !hasCount {
						separator := "|"
						if strings.Contains(td.ExpectedValue, "^") {
							separator = "^"
						}
						validValues := strings.Split(td.ExpectedValue, separator)
						if len(td.ExpectedValue) == 0 {
							// Special case: empty expected values means we accept any result or null
							return
						}
						replyStr := ""
						switch v := reply.(type) {
						case string:
							replyStr = v
						case int64:
							replyStr = strconv.FormatInt(v, 10)
						case int:
							replyStr = strconv.Itoa(v)
						case []byte:
							replyStr = string(v)
						case nil:
							replyStr = ""
						default:
							replyStr = fmt.Sprintf("%v", v)
						}

						found := false
						for _, val := range validValues {
							if val != "" && replyStr == val {
								found = true
								break
							}
						}
						if !found && replyStr != "" {
							t.Errorf("SRANDMEMBER returned '%s', but expected one of %v. Line: %d, Command: %s, Args: %v",
								replyStr, validValues, td.LineNumber, td.Command, td.Args)
						}
					} else {
						separator := "|"
						if strings.Contains(td.ExpectedValue, "^") {
							separator = "^"
						}
						validValues := strings.Split(td.ExpectedValue, separator)
						if len(td.ExpectedValue) == 0 {
							if replySlice, ok := reply.([]interface{}); ok {
								if len(replySlice) != 0 {
									t.Errorf("SRANDMEMBER expected empty array, got %v. Line: %d, Command: %s, Args: %v",
										replySlice, td.LineNumber, td.Command, td.Args)
								}
							} else {
								t.Errorf("Expected array response for SRANDMEMBER with count, got %T. Line: %d, Command: %s, Args: %v",
									reply, td.LineNumber, td.Command, td.Args)
							}
							return
						}

						if replySlice, ok := reply.([]interface{}); ok {
							for _, item := range replySlice {
								itemStr := ""
								switch v := item.(type) {
								case string:
									itemStr = v
								case int64:
									itemStr = strconv.FormatInt(v, 10)
								case int:
									itemStr = strconv.Itoa(v)
								case nil:
									itemStr = ""
								case []byte:
									itemStr = string(v)
								default:
									itemStr = fmt.Sprintf("%v", v)
								}

								found := false
								for _, val := range validValues {
									if val != "" && itemStr == val {
										found = true
										break
									}
								}
								if !found && itemStr != "" {
									t.Errorf("SRANDMEMBER returned '%s', but it's not in expected set %v. Line: %d, Command: %s, Args: %v",
										itemStr, validValues, td.LineNumber, td.Command, td.Args)
								}
							}
						} else if byteSlice, ok := reply.([][]byte); ok {
							for _, item := range byteSlice {
								itemStr := string(item)

								found := false
								for _, val := range validValues {
									if val != "" && itemStr == val {
										found = true
										break
									}
								}
								if !found && itemStr != "" {
									t.Errorf("SRANDMEMBER returned '%s', but it's not in expected set %v. Line: %d, Command: %s, Args: %v",
										itemStr, validValues, td.LineNumber, td.Command, td.Args)
								}
							}
						} else {
							t.Errorf("Expected array response for SRANDMEMBER with count, got %T. Line: %d, Command: %s, Args: %v",
								reply, td.LineNumber, td.Command, td.Args)
						}
					}
				}
			case "array":
				if err != nil {
					t.Errorf("Unexpected error: %v. Line: %d, Command: %s, Args: %v", err, td.LineNumber, td.Command, td.Args)
					return
				}

				replySlice, ok := reply.([]interface{})
				if !ok {
					if replyStr, ok := reply.(string); ok {
						if td.ExpectedValue == "[]" && replyStr == "" {
						} else {
							t.Errorf("Expected array, got string: %s. Line: %d, Command: %s, Args: %v", replyStr, td.LineNumber, td.Command, td.Args)
						}
						return
					} else if replyBytes, ok := reply.([]byte); ok {
						if td.ExpectedValue == "[]" && len(replyBytes) == 0 {
						} else {
							t.Errorf("Expected array, got bytes: %s. Line: %d, Command: %s, Args: %v", string(replyBytes), td.LineNumber, td.Command, td.Args)
						}
						return
					}
					t.Errorf("Expected array, got %T. Line: %d, Command: %s, Args: %v", reply, td.LineNumber, td.Command, td.Args)
					return
				}

				expectedParsed, err := parseNestedArrayString(td.ExpectedValue)
				if err != nil {
					t.Errorf("Error parsing expected array: %v. Line: %d, Command: %s, Args: %v", err, td.LineNumber, td.Command, td.Args)
					return
				}

				if !compareArrays(t, replySlice, expectedParsed) {
					t.Errorf("Array mismatch: expected %v, got %v. Line: %d, Command: %s, Args: %v", expectedParsed, replySlice, td.LineNumber, td.Command, td.Args)
				}
			case "nil":
				if reply != nil {
					t.Errorf("Expected nil result, got %v. Line: %d, Command: %s, Args: %v", reply, td.LineNumber, td.Command, td.Args)
				}
			case "int_pos":
				if err != nil {
					t.Errorf("Unexpected error: %v. Line: %d, Command: %s, Args: %v", err, td.LineNumber, td.Command, td.Args)
					return
				}
				var n int64
				if replyInt, ok := reply.(int64); ok {
					n = replyInt
				} else if replyInt, ok := reply.(int); ok {
					n = int64(replyInt)
				} else {
					t.Errorf("Expected int64/int, got %T. Line: %d, Command: %s, Args: %v", reply, td.LineNumber, td.Command, td.Args)
					return
				}

				if td.ExpectedValue == "positive" {
					if n <= 0 {
						t.Errorf("Expected positive integer, got %d. Line: %d, Command: %s, Args: %v", n, td.LineNumber, td.Command, td.Args)
					}
				} else {
					expected, err := strconv.ParseInt(td.ExpectedValue, 10, 64)
					if err != nil {
						t.Errorf("Could not parse expected value '%s': %v. Line: %d, Command: %s, Args: %v", td.ExpectedValue, err, td.LineNumber, td.Command, td.Args)
					} else if n != expected {
						t.Errorf("Expected %d, got %d. Line: %d, Command: %s, Args: %v", expected, n, td.LineNumber, td.Command, td.Args)
					}
				}
			case "float":
				if err != nil {
					t.Errorf("Unexpected error: %v. Line: %d, Command: %s, Args: %v", err, td.LineNumber, td.Command, td.Args)
					return
				}
				var f float64
				if replyFloat, ok := reply.(float64); ok {
					f = replyFloat
				} else if replyInt, ok := reply.(int64); ok {
					f = float64(replyInt)
				} else if replyInt, ok := reply.(int); ok {
					f = float64(replyInt)
				} else {
					if replyStr, ok := reply.([]byte); ok {
						parsedF, err := strconv.ParseFloat(string(replyStr), 64)
						if err != nil {
							t.Errorf("Could not parse float from bytes '%s': %v. Line: %d, Command: %s, Args: %v", string(replyStr), err, td.LineNumber, td.Command, td.Args)
							return
						}
						f = parsedF
					} else if replyStr, ok := reply.(string); ok {
						parsedF, err := strconv.ParseFloat(replyStr, 64)
						if err != nil {
							t.Errorf("Could not parse float from string '%s': %v. Line: %d, Command: %s, Args: %v", replyStr, err, td.LineNumber, td.Command, td.Args)
							return
						}
						f = parsedF
					} else {
						t.Errorf("Expected float64/int64/int/bytes/string, got %T. Line: %d, Command: %s, Args: %v", reply, td.LineNumber, td.Command, td.Args)
						return
					}
				}

				expected, err := strconv.ParseFloat(td.ExpectedValue, 64)
				if err != nil {
					t.Errorf("Could not parse expected value '%s': %v. Line: %d, Command: %s, Args: %v", td.ExpectedValue, err, td.LineNumber, td.Command, td.Args)
				} else if f != expected {
					t.Errorf("Expected %f, got %f. Line: %d, Command: %s, Args: %v", expected, f, td.LineNumber, td.Command, td.Args)
				}
			default:
				if td.ResultType == "" && td.ExpectedValue == "" {
					t.Errorf("Empty result type and expected value - likely parsing issue. Line: %d, Command: %s, Args: %v", td.LineNumber, td.Command, td.Args)
				} else {
					t.Errorf("Unknown result type: '%s', expected value: '%s'. Line: %d, Command: %s, Args: %v", td.ResultType, td.ExpectedValue, td.LineNumber, td.Command, td.Args)
				}
			}
		})
	}
}
