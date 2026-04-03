package dashcore

import (
	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/sync2"
	"github.com/zuoyebang/bitalostored/dashboard/models"
	"time"
)

func (s *DashCore) CreateDk(dk *models.DkItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	if ctx.dk[dk.Key] != nil {
		return errors.Errorf("dk-[%s] already exists", dk.Key)
	}
	defer s.dirtyDkCache(dk.Key)
	err = s.createDkProxy(ctx, dk)
	if err != nil {
		return err
	}
	dk.GenerateGroupKeys()
	storeErr := s.storeCreateDk(dk)
	_ = s.FillDks(ctx, []*models.DkItem{dk})
	return storeErr
}

func (s *DashCore) createDkProxy(ctx *context, dk *models.DkItem) error {
	var err error
	i := 0
	for _, p := range ctx.proxy {
		err = s.newProxyClient(p).CreateDk(dk)
		if err != nil {
			log.ErrorErrorf(err, "proxy-[%s] create dk failed", p.Token)
			if i > 3 {
				break
			}
			time.Sleep(10 * time.Millisecond)
			i++
			continue
		}
		break
	}
	return err
}

func (s *DashCore) RemoveDk(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	if ctx.dk[key] == nil {
		return errors.Errorf("dk-[%s] not exists", key)
	}
	defer s.dirtyDkCache(key)
	err = s.removeDkProxy(ctx, key)
	if err != nil {
		return err
	}
	return s.storeRemoveDk(key)
}

func (s *DashCore) removeDkProxy(ctx *context, key string) error {
	for _, p := range ctx.proxy {
		err := s.newProxyClient(p).RemoveDk(key)
		if err != nil {
			log.ErrorErrorf(err, "proxy-[%s] remove dk failed", p.Token)
			return err
		}
		break
	}
	dks := make([]*models.DkItem, 1)
	dks[0] = &models.DkItem{
		Key: key,
	}
	return s.FillDks(ctx, dks)
}

func (s *DashCore) FillDks(ctx *context, dks []*models.DkItem) error {
	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			err := s.newProxyClient(p).FillDk(dks)
			if err != nil {
				log.ErrorErrorf(err, "proxy-[%s] remove dk local cache failed", p.Token)
			}
			fut.Done(p.Token, err)
		}(p)
	}
	for t, v := range fut.Wait() {
		switch err := v.(type) {
		case error:
			if err != nil {
				return errors.Errorf("proxy-[%s] remove dk local cache failed", t)
			}
		}
	}
	return nil
}

func (s *DashCore) GetDKList() (map[string]*models.DkItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	return ctx.dk, nil
}
