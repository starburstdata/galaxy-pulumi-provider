package main

import (
	"context"
	"fmt"
	"os"
	"io"
	"net/http"
	"bytes"
	"encoding/json"

	p "github.com/pulumi/pulumi-go-provider"
	"github.com/pulumi/pulumi-go-provider/infer"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	"github.com/pulumi/pulumi/sdk/v3/go/common/tokens"
)

func main() {
	err := p.RunProvider("galaxy", "0.1.0", provider())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
		os.Exit(1)
	}
}

func provider() p.Provider {
	return infer.Provider(infer.Options{
		Resources: []infer.InferredResource{infer.Resource[*Cluster, ClusterArgs, ClusterState]()},
		ModuleMap: map[tokens.ModuleName]tokens.ModuleName{
			"galaxy": "index",
		},
	})
}

type Cluster struct{}

var _ = (infer.CustomDelete[ClusterState])((*Cluster)(nil))
var _ = (infer.CustomCheck[ClusterArgs])((*Cluster)(nil))
var _ = (infer.CustomUpdate[ClusterArgs, ClusterState])((*Cluster)(nil))
var _ = (infer.CustomDiff[ClusterArgs, ClusterState])((*Cluster)(nil))
var _ = (infer.CustomRead[ClusterArgs, ClusterState])((*Cluster)(nil))
var _ = (infer.ExplicitDependencies[ClusterArgs, ClusterState])((*Cluster)(nil))
var _ = (infer.Annotated)((*Cluster)(nil))
var _ = (infer.Annotated)((*ClusterArgs)(nil))
var _ = (infer.Annotated)((*ClusterState)(nil))

func (f *Cluster) Annotate(a infer.Annotator) {
	a.Describe(&f, "A file projected into a pulumi resource")
}

type ClusterArgs struct {
	Name    string `pulumi:"name"`
	CloudRegionId   string   `pulumi:"cloudRegionId"`
	MinWorkers int `pulumi:"minWorkers"`
	MaxWorkers int `pulumi:"maxWorkers"`
}

func (f *ClusterArgs) Annotate(a infer.Annotator) {
	a.Describe(&f.Name, "The name of the Galaxy cluster.")
	a.Describe(&f.CloudRegionId, "Cloud region ID of where the cluster will be created.")
	a.Describe(&f.MinWorkers, "Minimum worker count.")
	a.Describe(&f.MaxWorkers, "Maximum worker count.")
}

type ClusterState struct {
	ClusterArgs
	ClusterId string `pulumi:"clusterId"`
}

func (f *ClusterState) Annotate(a infer.Annotator) {
	a.Describe(&f.ClusterArgs, "The provided cluster fields.")
	a.Describe(&f.ClusterId, "Cluster id of the created cluster.")
}

func (*Cluster) Create(ctx context.Context, name string, input ClusterArgs, preview bool) (id string, output ClusterState, err error) {
	if preview { // Don't do the actual creating if in preview
		return name, ClusterState{ClusterArgs: input, ClusterId: "<tbd>"}, nil
	}

	// make rest call to create cluster
	jsonPayload, err := toGalaxyClusterJson(input)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", "https://starbursttelemetry.galaxy.starburst.io/public/api/v1/cluster", bytes.NewBuffer(jsonPayload))
	if err != nil {
		panic(err)
	}

	resp, err := callGalaxy(req)
	if err != nil {
		panic(err)
	}

	var createdCluster ExistingGalaxyCluster
	err = json.Unmarshal([]byte(resp), &createdCluster)
	if err != nil {
		panic(err)
	}

	return name, ClusterState{
		ClusterArgs: input, 
		ClusterId: createdCluster.ClusterId,
	}, nil
}

func (*Cluster) Delete(ctx context.Context, id string, props ClusterState) error {

	req, err := http.NewRequest("DELETE", "https://starbursttelemetry.galaxy.starburst.io/public/api/v1/cluster/" + props.ClusterId, nil)
	if err != nil {
		return err
	}

	_, err = callGalaxy(req)
	if err != nil {
		return err
	}

	return nil
}

func (*Cluster) Check(ctx context.Context, name string, oldInputs, newInputs resource.PropertyMap) (ClusterArgs, []p.CheckFailure, error) {
	return infer.DefaultCheck[ClusterArgs](ctx, newInputs)
}

func (*Cluster) Update(ctx context.Context, id string, olds ClusterState, news ClusterArgs, preview bool) (ClusterState, error) {
	if !preview && olds.ClusterArgs != news {
		// update cluster

		jsonPayload, err := toGalaxyClusterJson(news)
		if err != nil {
			panic(err)
		}
	
		req, err := http.NewRequest("PATCH", "https://starbursttelemetry.galaxy.starburst.io/public/api/v1/cluster/" + olds.ClusterId, bytes.NewBuffer(jsonPayload))
		if err != nil {
			panic(err)
		}

		resp, err := callGalaxy(req)
		if err != nil {
			panic(err)
		}

		var createdCluster ExistingGalaxyCluster
		err = json.Unmarshal([]byte(resp), &createdCluster)
		if err != nil {
			panic(err)
		}
	
		return ClusterState{
			ClusterArgs: news, 
			ClusterId: createdCluster.ClusterId,
		}, nil
	}

	return ClusterState{}, nil

}

func (*Cluster) Diff(ctx context.Context, id string, olds ClusterState, news ClusterArgs) (p.DiffResponse, error) {
	diff := map[string]p.PropertyDiff{}
	if news.Name != olds.ClusterArgs.Name {
		diff["name"] = p.PropertyDiff{Kind: p.Update}
	}
	if news.CloudRegionId != olds.ClusterArgs.CloudRegionId {
		diff["cloudRegionId"] = p.PropertyDiff{Kind: p.Update}
	}
	if news.MinWorkers != olds.ClusterArgs.MinWorkers {
		diff["minWorkers"] = p.PropertyDiff{Kind: p.Update}
	}
	if news.MaxWorkers != olds.ClusterArgs.MaxWorkers {
		diff["maxWorkers"] = p.PropertyDiff{Kind: p.Update}
	}
	return p.DiffResponse{
		DeleteBeforeReplace: false,
		HasChanges:          len(diff) > 0,
		DetailedDiff:        diff,
	}, nil
}

func (*Cluster) Read(ctx context.Context, id string, inputs ClusterArgs, state ClusterState) (canonicalID string, normalizedInputs ClusterArgs, normalizedState ClusterState, err error) {

	// if err != nil {
	// 	return "", ClusterArgs{}, ClusterState{}, err
	// }
	return id, inputs, ClusterState{
			ClusterArgs:    inputs,
			ClusterId:   id,
		}, nil
}

func (*Cluster) WireDependencies(f infer.FieldSelector, args *ClusterArgs, state *ClusterState) {
	f.OutputField(&state.ClusterArgs).DependsOn(f.InputField(&args))
}

type GalaxyCluster struct {
	Name    string `json:"name"`
	CloudRegionId   string   `json:"cloudRegionId"`
	CatalogRefs []string `json:"catalogRefs"`
	MinWorkers int `json:"minWorkers"`
	MaxWorkers int `json:"maxWorkers"`
	WarpResiliencyEnabled bool `json:"warpResiliencyEnabled"`
	ResultCacheEnabled bool `json:"resultCacheEnabled"`
	PrivateLinkCluster bool `json:"privateLinkCluster"`
}

type ExistingGalaxyCluster struct {
	ClusterId string `json:"clusterId"`
	Name    string `json:"name"`
	CloudRegionId   string   `json:"cloudRegionId"`
	CatalogRefs []string `json:"catalogRefs"`
	MinWorkers int `json:"minWorkers"`
	MaxWorkers int `json:"maxWorkers"`
	WarpResiliencyEnabled bool `json:"warpResiliencyEnabled"`
	ResultCacheEnabled bool `json:"resultCacheEnabled"`
	PrivateLinkCluster bool `json:"privateLinkCluster"`
}

func toGalaxyClusterJson(c ClusterArgs) ([]byte, error) {
	gc := GalaxyCluster{
		Name: c.Name,
		CloudRegionId: c.CloudRegionId,
		CatalogRefs: []string{},
		MinWorkers: c.MinWorkers,
		MaxWorkers: c.MaxWorkers,
		WarpResiliencyEnabled: false,
		ResultCacheEnabled: false,
		PrivateLinkCluster: false,
	}
	return json.Marshal(gc)
}

func callGalaxy(req *http.Request) (response string, err error) {

	galaxyToken := getGalaxyToken()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer " + galaxyToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err			
	}
	if resp.StatusCode <= 300 {
		return string(body), nil
	} else {
		println(resp.StatusCode, string(body))
		panic(string(body))
	}
	defer resp.Body.Close()
	return "", nil
}

type GalaxyToken struct {
	AccessToken string `json:"access_token"`
}

func getGalaxyToken() (token string) {
	idSecret := os.Getenv("GALAXY_CLIENT_ID_SECRET")
	reqBody := "grant_type=client_credentials"

	req, err := http.NewRequest("POST", "https://starbursttelemetry.galaxy.starburst.io/oauth/v2/token", bytes.NewBuffer([]byte(reqBody)))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Basic " + idSecret)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode == http.StatusOK {
		var token GalaxyToken
		var _ = json.Unmarshal([]byte(body), &token)
		return token.AccessToken
	} else {
		println(resp.StatusCode, string(body))
		panic(string(body))
	}
	defer resp.Body.Close()
	return ""
}