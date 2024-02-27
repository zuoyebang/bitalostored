package cmd_test

import (
	"strings"
	"testing"

	"github.com/zuoyebang/bitalostored/stored/internal/resp"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"

	"github.com/zuoyebang/bitalostored/stored/server"
)

func TestEval(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if ok, err := redis.Int(c.Do("eval", "return 42", "0")); err != nil {
		t.Fatal(err)
	} else if ok != 42 {
		t.Fatal(ok)
	}

	if ok, err := redis.Strings(c.Do("eval", "return {KEYS[1], ARGV[1]}", "1", "key1", "argv1")); err != nil {
		t.Fatal(err)
	} else if !assert.Equal(t, "key1", ok[0]) || !assert.Equal(t, "argv1", ok[1]) {
		t.Fatal(ok)
	}

	if _, err := redis.String(c.Do("eval", "42", "0")); err == nil {
		t.Fatal("error return")
	} else if !strings.Contains(err.Error(), "ERR Error compiling script") {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("eval", "return 42")); err == nil {
		t.Fatal("error return")
	} else if err.Error() != "ERR wrong number of arguments for 'eval' command" {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("eval", "os.exit(42)")); err == nil {
		t.Fatal("error return")
	} else if err.Error() != "ERR wrong number of arguments for 'eval' command" {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("eval", `return string.gsub("foo", "o", "a")`)); err == nil {
		t.Fatal("error return")
	} else if err.Error() != "ERR wrong number of arguments for 'eval' command" {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("eval", "return 42", "1")); err == nil {
		t.Fatal("error return")
	} else if err.Error() != "ERR Number of keys can't be greater than number of args" {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("eval", "return 42", "-1")); err == nil {
		t.Fatal("error return")
	} else if err.Error() != "ERR Number of keys can't be negative" {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("eval", "42", "letter")); err == nil {
		t.Fatal("error return")
	} else if err.Error() != "ERR value is not an integer or out of range" {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("eval", "someGlobal = 5", "0")); err == nil {
		t.Fatal("error return")
	} else if !strings.Contains(err.Error(), "Script attempted to create global variable 'someGlobal'") {
		t.Fatal(err)
	}
}

func TestEvalCall(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if _, err := redis.String(c.Do("eval", "redis.call()", "0")); err == nil {
		t.Fatal("error return")
	} else if !strings.Contains(err.Error(), "ERR Error compiling script") {
		t.Fatal(err)
	}

	if ok, err := redis.String(c.Do("eval", "return redis.call('set','{test}foooo','bar')", "0")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	if _, err := redis.String(c.Do("eval", "redis.call('set',KEYS[1],ARGV[1])", "1", "{test}foo", "bar")); err == nil {
		t.Fatal("error return")
	} else if err != redis.ErrNil {
		t.Fatal(err)
	}

	if ok, err := redis.String(c.Do("eval", "return redis.call('get',KEYS[1])", "1", "{test}foo")); err != nil {
		t.Fatal(err)
	} else if ok != "bar" {
		t.Fatal(ok)
	}

	if ok, err := redis.Int64(c.Do("eval", "return redis.call('del',KEYS[1])", "1", "{test}foo")); err != nil {
		t.Fatal(err)
	} else if ok != 1 {
		t.Fatal(ok)
	}

	if ok, err := redis.Int64(c.Do("eval", "return redis.call('del',KEYS[1])", "1", "{test}foo")); err != nil {
		t.Fatal(err)
	} else if ok != 0 {
		t.Fatal(ok)
	}
}

func TestScript(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	var (
		script1 = `
local value = redis.call('get', KEYS[1]);
if (value == ARGV[1]) then
	redis.call('del', KEYS[1]);
	return 1;
end;
return 0;
`
		script2 = `
for i=1,tonumber(KEYS[1]),1 do
    redis.call('del', ARGV[i]);
end 
return 1;
`
		script3 = `
local result = redis.call('set', KEYS[1], ARGV[1], 'nx', 'ex', ARGV[2])
if result then
	return 1
else 
	return 0
end;
`
		script1sha = "47d7fdd6f539c810ca8bced3561355f298448e6c"
		script2sha = "da3ddb000db44dd888c4c55380167a2a9361688c"
		script3sha = "c07f9c987b2ae1e3cdedf93fbfba61e7595e733e"
	)
	if ok, err := redis.String(c.Do("SCRIPT", "LOAD", script1)); err != nil {
		t.Fatal(err)
	} else if ok != script1sha {
		t.Fatal(ok)
	}
	if ok, err := redis.String(c.Do("SCRIPT", "LOAD", script2)); err != nil {
		t.Fatal(err)
	} else if ok != script2sha {
		t.Fatal(ok)
	}
	if ok, err := redis.String(c.Do("SCRIPT", "LOAD", script3)); err != nil {
		t.Fatal(err)
	} else if ok != script3sha {
		t.Fatal(ok)
	}
	if ok, err := redis.Ints(c.Do("SCRIPT", "EXISTS", script1sha, script2sha, script3sha, "invalid sha")); err != nil {
		t.Fatal(err)
	} else if len(ok) != 4 {
		t.Fatal(ok)
	} else if ok[0] != 1 || ok[1] != 1 || ok[2] != 1 || ok[3] != 0 {
		t.Fatal(ok)
	}
	if _, err := redis.String(c.Do("SCRIPT", "FLUSH")); err != nil {
		t.Fatal(err)
	}

	if ok, err := redis.Ints(c.Do("SCRIPT", "EXISTS", script1sha, script2sha, script3sha, "invalid sha")); err != nil {
		t.Fatal(err)
	} else if len(ok) != 4 {
		t.Fatal(ok)
	} else if ok[0] != 0 || ok[1] != 0 || ok[2] != 0 || ok[3] != 0 {
		t.Fatal(ok)
	}

	if _, err := redis.String(c.Do("SCRIPT")); err == nil {
		t.Fatal("error return")
	} else if err.Error() != "ERR wrong number of arguments for 'script' command" {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("SCRIPT", "LOAD")); err == nil {
		t.Fatal("error return")
	} else if err.Error() != "ERR Unknown subcommand or wrong number of arguments for 'LOAD'. Try SCRIPT HELP." {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("SCRIPT", "LOAD", "return 42", "FOO")); err == nil {
		t.Fatal("error return")
	} else if err.Error() != "ERR Unknown subcommand or wrong number of arguments for 'LOAD'. Try SCRIPT HELP." {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("SCRIPT", "LOAD", "[")); err == nil {
		t.Fatal("error return")
	} else if err.Error() != "ERR Error compiling script (new function):L user_script line:1(column:1) near '[':   syntax error" {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("SCRIPT", "FOO")); err == nil {
		t.Fatal("error return")
	} else if err.Error() != "ERR empty command for 'scriptfoo' command" {
		t.Fatal(err)
	}
}

func TestCJSON(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	if ok, err := redis.String(c.Do("EVAL", `return cjson.decode('{"id":"foo"}')['id']`, "0")); err != nil {
		t.Fatal(err)
	} else if ok != "foo" {
		t.Fatal(err)
	}
	if ok, err := redis.String(c.Do("EVAL", `return cjson.encode({foo=42})`, "0")); err != nil {
		t.Fatal(err)
	} else if ok != `{"foo":42}` {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("EVAL", `return redis.encode()`, "0")); err == nil {
		t.Fatal("error return")
	} else if !strings.Contains(err.Error(), "Error compiling script") {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("EVAL", `return redis.encode("1", "2")`, "0")); err == nil {
		t.Fatal("error return")
	} else if !strings.Contains(err.Error(), "Error compiling script") {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("EVAL", `return redis.decode()`, "0")); err == nil {
		t.Fatal("error return")
	} else if !strings.Contains(err.Error(), "Error compiling script") {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("EVAL", `return redis.encode("1", "2")`, "0")); err == nil {
		t.Fatal("error return")
	} else if !strings.Contains(err.Error(), "Error compiling script") {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("EVAL", `return redis.decode("{")`, "0")); err == nil {
		t.Fatal("error return")
	} else if !strings.Contains(err.Error(), "Error compiling script") {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("EVAL", `return redis.decode("1", "2")`, "0")); err == nil {
		t.Fatal("error return")
	} else if !strings.Contains(err.Error(), "Error compiling script") {
		t.Fatal(err)
	}
}

func TestSha1Hex(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if ok, err := redis.String(c.Do("EVAL", `return redis.sha1hex("foo")`, "0")); err != nil {
		t.Fatal(err)
	} else if ok != "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33" {
		t.Fatal(err)
	}
	if ok, err := redis.String(c.Do("EVAL", `return redis.sha1hex(42)`, "0")); err != nil {
		t.Fatal(err)
	} else if ok != "92cfceb39d57d914ed8b14d0e37643de0797ae56" {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("EVAL", "redis.sha1hex()", "0")); err == nil {
		t.Fatal("error return")
	} else if !strings.Contains(err.Error(), "wrong number of arguments") {
		t.Fatal(err)
	}
}

func TestEvalsha(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	script1sha := "bfbf458525d6a0b19200bfd6db3af481156b367b"
	if ok, err := redis.String(c.Do("SCRIPT", "LOAD", "return {KEYS[1],ARGV[1]}")); err != nil {
		t.Fatal(err)
	} else if ok != script1sha {
		t.Fatal(ok)
	}

	if ok, err := redis.Strings(c.Do("evalsha", script1sha, "1", "{test}key1", "argv1")); err != nil {
		t.Fatal(err)
	} else if !assert.Equal(t, "{test}key1", ok[0]) || !assert.Equal(t, "argv1", ok[1]) {
		t.Fatal(ok)
	}

	if _, err := redis.String(c.Do("evalsha", "{test}foo")); !(err != nil && err.Error() == server.ErrWrongNumber("EVALSHA")) {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("evalsha", "{test}foo", "0")); !(err != nil && err.Error() == server.MsgNoScriptFound) {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("evalsha", script1sha, script1sha)); !(err != nil && err.Error() == server.MsgInvalidInt) {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("evalsha", script1sha, -1)); !(err != nil && err.Error() == server.MsgNegativeKeysNumber) {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("evalsha", script1sha, 1)); !(err != nil && err.Error() == server.MsgInvalidKeysNumber) {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("evalsha", "{test}foo", 1, "bar")); !(err != nil && err.Error() == server.MsgNoScriptFound) {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("evalsha", "{test}foo", 1, "bar")); !(err != nil && err.Error() == server.MsgNoScriptFound) {
		t.Fatal(err)
	}

}

func TestCmdEvalReply(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if _, err := redis.String(c.Do("eval", "", "0")); err == nil {
		t.Fatal("err return")
	} else if err != redis.ErrNil {
		t.Fatal(err)
	}

	if ok, err := redis.Bool(c.Do("eval", "return true", "0")); err != nil {
		t.Fatal(err)
	} else if ok != true {
		t.Fatal(ok)
	}

	if ok, _ := redis.Bool(c.Do("eval", "return false", "0")); ok != false {
		t.Fatal(ok)
	} else if ok != false {
		t.Fatal(ok)
	}

	if ok, err := redis.Int(c.Do("eval", "return 10", "0")); err != nil {
		t.Fatal(err)
	} else if ok != 10 {
		t.Fatal(ok)
	}

	if ok, err := redis.Int(c.Do("eval", "return 12.345", "0")); err != nil {
		t.Fatal(err)
	} else if ok != 12 {
		t.Fatal(ok)
	}

	if ok, err := redis.Int(c.Do("eval", "return 10,20", "0")); err != nil {
		t.Fatal(err)
	} else if ok != 10 {
		t.Fatal(ok)
	}

	if ok, err := redis.String(c.Do("eval", "return 'test'", "0")); err != nil {
		t.Fatal(err)
	} else if ok != "test" {
		t.Fatal(ok)
	}

	if ok, err := redis.Values(c.Do("eval", "return {10, 20, {30, 'test', true, 40}, false}", "0")); err != nil {
		t.Fatal(err)
	} else {
		if data, e := redis.Int(ok[0], nil); e != nil {
			t.Fatal(e)
		} else if data != 10 {
			t.Fatal(data)
		}
		if data, e := redis.Int(ok[1], nil); e != nil {
			t.Fatal(e)
		} else if data != 20 {
			t.Fatal(data)
		}
		if _, e := redis.Values(ok[2], nil); e != nil {
			t.Fatal(e)
		}
		if data, _ := redis.Bool(ok[3], nil); data != false {
			t.Fatal(data)
		}
	}

	if _, err := redis.String(c.Do("eval", "return {err='broken'}", "0")); !(err != nil && err.Error() == "broken") {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("eval", `return redis.error_reply("broken")`, "0")); !(err != nil && err.Error() == "broken") {
		t.Fatal(err)
	}

	if ok, err := redis.String(c.Do("eval", `return {ok="good"}`, "0")); err != nil {
		t.Fatal(err)
	} else if ok != "good" {
		t.Fatal(ok)
	}

	if ok, err := redis.String(c.Do("eval", `return redis.status_reply("good")`, "0")); err != nil {
		t.Fatal(err)
	} else if ok != "good" {
		t.Fatal(ok)
	}
}

func TestCmdEvalResponse(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if ok, err := redis.String(c.Do("eval", "return redis.call('set','{test}foo','bar')", "0")); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if ok, err := redis.String(c.Do("eval", "return redis.call('get','{test}foo')", "0")); err != nil {
		t.Fatal(err)
	} else if ok != "bar" {
		t.Fatal(ok)
	}

	if _, err := redis.String(c.Do("eval", "return redis.call('get','nosuch')", "0")); err != nil && err.Error() != "redigo: nil returned" {
		t.Fatal(err)
	}

	if ok, err := redis.String(c.Do("eval", "return redis.call('HMSET', '{test}mkey', 'foo','bar','foo1','bar1')", "0")); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}
}
