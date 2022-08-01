package client

import (
	"context"

	"github.com/CosmWasm/wasmd/x/wasm/types"
	"github.com/sirupsen/logrus"

	"google.golang.org/grpc"
)

type Client struct {
	log    *logrus.Logger
	client *grpc.ClientConn
}

func New(url string, log *logrus.Logger) (*Client, error) {
	log.Debugf("Connecting with grpc server: %s", url)

	grpcConn, err := grpc.Dial(
		url,
		grpc.WithInsecure(),
	)

	if err != nil {
		log.Error("Could not conntect with grpc server: ", err)
		return nil, err
	}

	return &Client{
		client: grpcConn,
		log:    log,
	}, nil
}

func (c *Client) Close() {
	c.log.Debug("Close grpc connection")
	c.client.Close()
}

func (c *Client) GetContractInfo(contractAddress string) error {
	c.log.Debugf("Get contract info for address: %s", contractAddress)

	queryClient := types.NewQueryClient(c.client)
	res, err := queryClient.ContractInfo(
		context.Background(),
		&types.QueryContractInfoRequest{
			Address: contractAddress,
		},
	)

	if err != nil {
		c.log.Errorf("can't get contract info, address: %s", contractAddress)
		return err
	}

	c.log.Debugf("Get contract info for %s response: %s", contractAddress, res)
	return nil
}
