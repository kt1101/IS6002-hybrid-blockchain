package driver

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	pbv "hybrid/proto/veritas"
	"hybrid/tso"
	"hybrid/veritas/db"
)

type Driver struct {
	signature string
	cc        *grpc.ClientConn
	dbCli     pbv.NodeClient
	tsCli     *tso.Client
}

func Open(serverAddr, tsoAddr, signature string) (*Driver, error) {
	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	dbCli := pbv.NewNodeClient(cc)

	// An empty tsoAddr means no TSO is used (e.g. veritastm): writes carry
	// Version 0 and no TSO client is created.
	var tsCli *tso.Client
	if tsoAddr != "" {
		tsCli, err = tso.NewClient(tsoAddr)
		if err != nil {
			cc.Close()
			return nil, err
		}
	}

	return &Driver{
		signature: signature,
		cc:        cc,
		dbCli:     dbCli,
		tsCli:     tsCli,
	}, nil
}

func (d *Driver) Begin() (*db.TransactionDB, error) {
	if d.tsCli == nil {
		return nil, errors.New("driver: transactions require a TSO address")
	}
	ts, err := d.tsCli.TS()
	if err != nil {
		return nil, err
	}
	return db.NewTransaction(ts, d.dbCli, d.signature), nil
}

func (d *Driver) Get(ctx context.Context, key string) (string, error) {
	res, err := d.dbCli.Get(ctx, &pbv.GetRequest{
		Signature: d.signature,
		Key:       key,
	})
	if err != nil {
		return "", err
	}
	return res.GetValue(), nil
}

func (d *Driver) Set(ctx context.Context, key, value string) error {
	var ts int64
	if d.tsCli != nil {
		var err error
		if ts, err = d.tsCli.TS(); err != nil {
			return err
		}
	}
	if _, err := d.dbCli.Set(ctx, &pbv.SetRequest{
		Signature: d.signature,
		Key:       key,
		Value:     value,
		Version:   ts,
	}); err != nil {
		return err
	}
	return nil
}

func (d *Driver) Close() error {
	return d.cc.Close()
}
