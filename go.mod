module github.com/itmo-eve/edge-containers

go 1.12

require (
	github.com/containerd/containerd v1.5.2
	github.com/docker/docker v17.12.1-ce+incompatible // indirect
	github.com/opencontainers/go-digest v1.0.0
	github.com/opencontainers/image-spec v1.0.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.1.3
	github.com/stretchr/testify v1.7.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	oras.land/oras-go v0.4.0
	rsc.io/letsencrypt v0.0.3 // indirect
)

replace github.com/docker/docker => github.com/docker/docker v0.7.3-0.20190826074503-38ab9da00309

replace golang.org/x/sys => golang.org/x/sys v0.0.0-20210124154548-22da62e12c0c
