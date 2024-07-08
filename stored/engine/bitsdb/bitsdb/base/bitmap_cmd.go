package base

import "github.com/zuoyebang/bitalostored/stored/internal/tclock"

func (bo *BaseObject) bitmapMemExpireAt(key []byte, when uint64) (int64, bool) {
	if bi, ok := bo.BaseDb.BitmapMem.Get(key); ok {
		if bi.Expired() {
			return 0, true
		} else {
			bi.SetExpire(uint64(when))
			return 1, true
		}
	}

	return 0, false
}

func (bo *BaseObject) bitmapMemTTL(key []byte) (int64, bool) {
	if bi, ok := bo.BaseDb.BitmapMem.Get(key); ok {
		expire := int64(bi.expireMs.Load())
		return checkTTL(expire), true
	}

	return 0, false
}

func checkTTL(expire int64) int64 {
	if expire == 0 {
		return ErrnoKeyPersist
	} else {
		nowtime := tclock.GetTimestampMilli()
		if expire <= nowtime {
			return ErrnoKeyNotFoundOrExpire
		} else {
			return int64(expire) - nowtime
		}
	}
}

func (bo *BaseObject) bitmapMemPersist(key []byte) (int64, bool) {
	if bi, ok := bo.BaseDb.BitmapMem.Get(key); ok {
		if bi.Expired() {
			return 0, true
		} else {
			bi.SetExpire(0)
			return 1, true
		}
	}

	return 0, false
}

func (bo *BaseObject) bitmapMemExists(key []byte) (int64, bool) {
	if bi, ok := bo.BaseDb.BitmapMem.Get(key); ok {
		if bi.Expired() {
			return 0, true
		}
		return 1, true
	}

	return 0, false
}
