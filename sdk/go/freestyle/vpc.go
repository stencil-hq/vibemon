package freestyle

import (
	"context"

	vmon "github.com/can1357/vibemon/sdk/go"
)

// Vpc provides routed private-network operations.
type Vpc struct{ freestyle *Freestyle }

// VpcCreateOptions configures a routed VPC.
type VpcCreateOptions struct {
	CIDR string
	Name string
}

// FreestyleVpc is the Freestyle-shaped VPC identity.
type FreestyleVpc struct{ VpcID string }

// CreatedVpc is the result of creating a routed VPC.
type CreatedVpc struct {
	VpcID string
	Vpc   FreestyleVpc
}

// VpcListEntry is one routed VPC collection row.
type VpcListEntry struct {
	VpcID string
	Name  string
	CIDR  string
}

// Create provisions a routed VPC.
func (vpc *Vpc) Create(ctx context.Context, options *VpcCreateOptions) (CreatedVpc, error) {
	client, err := vpc.freestyle.client()
	if err != nil {
		return CreatedVpc{}, err
	}
	var name, cidr string
	if options != nil {
		name, cidr = options.Name, options.CIDR
	}
	created, err := client.Vpcs().Create(ctx, vmon.VPCCreateOptions{Name: name, CIDR: cidr})
	if err != nil {
		return CreatedVpc{}, err
	}
	return CreatedVpc{VpcID: created.ID, Vpc: FreestyleVpc{VpcID: created.ID}}, nil
}

// List returns routed VPCs.
func (vpc *Vpc) List(ctx context.Context) ([]VpcListEntry, error) {
	client, err := vpc.freestyle.client()
	if err != nil {
		return nil, err
	}
	rows, err := client.Vpcs().List(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]VpcListEntry, 0, len(rows))
	for _, row := range rows {
		result = append(result, VpcListEntry{VpcID: row.ID, Name: row.Name, CIDR: row.CIDR})
	}
	return result, nil
}

// Delete removes an unattached routed VPC.
func (vpc *Vpc) Delete(ctx context.Context, vpcID string) error {
	client, err := vpc.freestyle.client()
	if err != nil {
		return err
	}
	return client.Vpcs().Delete(ctx, vpcID)
}
