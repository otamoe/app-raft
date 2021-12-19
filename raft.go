package appraft

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	dragonboat "github.com/lni/dragonboat/v3"
	dconfig "github.com/lni/dragonboat/v3/config"
	appconfig "github.com/otamoe/app-config"
)

var NodeHost *dragonboat.NodeHost

func GetNodeHost() *dragonboat.NodeHost {
	return NodeHost
}

func SetNodeHost(nh *dragonboat.NodeHost) {
	NodeHost = nh
	return
}

func NodeHostClose() error {
	NodeHost.Stop()
	return nil
}

func DefaultNodeHostConfig() (nhc dconfig.NodeHostConfig, err error) {

	// 部署 id
	nhc.DeploymentID = appconfig.GetUint64("raft.deploymentID")

	// raft 地址
	nhc.RaftAddress = appconfig.GetString("raft.address")

	// rtt 延迟
	if nhc.RTTMillisecond == 0 {
		nhc.RTTMillisecond = 2000
	}

	// 健康指标
	nhc.EnableMetrics = true

	// 地址绑定 host id
	// nhc.AddressByNodeHostID = true

	// 最大发送队列字节
	nhc.MaxSendQueueSize = 1024 * 1024 * 256

	// 最大接收队列字节
	nhc.MaxReceiveQueueSize = 1024 * 1024 * 256

	// tls 配置
	if len(appconfig.GetString("raft.tls.ca")) != 0 || len(appconfig.GetString("raft.tls.cert")) != 0 || len(appconfig.GetString("raft.tls.key")) != 0 {
		nhc.MutualTLS = true

		// ca 文件
		if nhc.CAFile == "" && len(appconfig.GetString("raft.tls.ca")) != 0 {
			nhc.CAFile = path.Join(os.TempDir(), fmt.Sprintf("raft-tls-ca-%d", AddrRaftNodeIDP(nhc.RaftAddress)))
			if err = ioutil.WriteFile(nhc.CAFile, []byte(appconfig.GetString("raft.tls.ca")), 0755); err != nil {
				return
			}
		}

		// cert 文件
		if nhc.CertFile == "" && len(appconfig.GetString("raft.tls.cert")) != 0 {
			nhc.CertFile = path.Join(os.TempDir(), fmt.Sprintf("raft-tls-cert-%d", AddrRaftNodeIDP(nhc.RaftAddress)))
			if err = ioutil.WriteFile(nhc.CertFile, []byte(appconfig.GetString("raft.tls.cert")), 0755); err != nil {
				return
			}
		}

		// key 文件
		if nhc.KeyFile == "" && len(appconfig.GetString("raft.tls.key")) != 0 {
			nhc.KeyFile = path.Join(os.TempDir(), fmt.Sprintf("raft-tls-key-%d", AddrRaftNodeIDP(nhc.RaftAddress)))
			if err = ioutil.WriteFile(nhc.KeyFile, []byte(appconfig.GetString("raft.tls.key")), 0755); err != nil {
				return
			}
		}
	}

	return
}

func init() {
	appconfig.SetDefault("raft.deploymentID", 1, "raft deployment ID")
	appconfig.SetDefault("raft.nodeID", 1, "raft node ID")
	appconfig.SetDefault("raft.address", "127.0.0.1:6501", "raft address")
	appconfig.SetDefault("raft.tls.ca", "", "raft tls ca")
	appconfig.SetDefault("raft.tls.cert", "", "raft tls cert")
	appconfig.SetDefault("raft.tls.key", "", "raft tls key")
}
