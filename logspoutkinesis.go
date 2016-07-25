/*
 * Copyright 2013-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Based on:
 * https://github.com/rtoma/logspout-redis-logstash/blob/master/redis.go
*/
 
package logspoutkinesis

import (
    "fmt"
    "strings"
    "strconv"
    "log"
    "os"
    "time"
    "encoding/json"
    "github.com/gliderlabs/logspout/router"
    kinesis "github.com/sendgridlabs/go-kinesis"
    batchproducer "github.com/sendgridlabs/go-kinesis/batchproducer"
)

type KinesisAdapter struct {
    route       	*router.Route
    batch_client	*kinesis.Kinesis
    batch_producer 	batchproducer.Producer
    streamName		string
    docker_host 	string
    partition_key   string
    use_v0    		bool
}

type DockerFields struct {
    Name        string              `json:"name"`
    CID         string              `json:"cid"`
    Image       string              `json:"image"`
    ImageTag    string              `json:"image_tag,omitempty"`
    ImageRepo   string              `json:"image_repo,omitempty"`
    Source      string              `json:"source"`
    DockerHost  string              `json:"docker_host,omitempty"`
    Labels      map[string]string   `json:"labels,omitempty"`
}

type LogstashFields struct {
    Docker      DockerFields    `json:"docker"`
}

type LogstashMessageV0 struct {
    Timestamp   string            `json:"@timestamp"`
    Sourcehost  string            `json:"@source_host"`
    Message     string            `json:"@message"`
    Fields      LogstashFields    `json:"@fields"`
}

type LogstashMessageV1 struct {
    Timestamp   string            `json:"@timestamp"`
    Sourcehost  string            `json:"host"`
    Message     string            `json:"message"`
    Fields      DockerFields      `json:"docker"`
}

type RouteHelper struct {
    route     *router.Route
}

func init() {
	// Register this adapter
    router.AdapterFactories.Register(NewLogspoutAdapter, "kinesis")
}

func NewLogspoutAdapter(route *router.Route) (router.LogAdapter, error) {
	// kinesis client
    batch_client := getKinesis(route)

    // The kinesis stream where the logs should be sent to
    streamName := route.Address
    fmt.Printf("# KINESIS Adapter - Using stream: %s\n", streamName)
    
    // Create a route helper for easier route access
    route_helper := RouteHelper{route: route}
    
    // Batch config
    batchproducer_config := getKinesisConfig(route_helper)
    fmt.Printf("# KINESIS Adapter - Batch config: %v\n", batchproducer_config)
    
	// Create a batchproducer
	batch_producer, err := batchproducer.New(batch_client, streamName, batchproducer_config)
	if err != nil {
		fmt.Printf("Unable to retrieve create batchproducer: %v", err)
		os.Exit(1)
	}

	// Host of the docker instance
    docker_host := route.Options["docker_host"]
    if docker_host == "" {
        docker_host = getEnvVar("LK_DOCKER_HOST", "unknown-docker-host")
    }
	
	// Whether to use the v0 logtstash layout or v1
    use_v0 := route.Options["use_v0_layout"] != ""
    if !use_v0 {
        use_v0 = getEnvVar("LK_USE_V0_LAYOUT", "") != ""
    }

    // Partition key
    partition_key := route.Options["partition_key"]
    if partition_key == "" {
        partition_key = getEnvVar("LK_PARTITION_KEY", "")
        if partition_key == "" {
            partition_key = docker_host
        }
    }

	// Return the kinesis adapter that will receive all the logs
	return &KinesisAdapter{
        route:          route,
        batch_producer: batch_producer,
        streamName: 	streamName,
        docker_host:    docker_host,
        partition_key:  partition_key,
        use_v0:         use_v0,
    }, nil
}

func getKinesis(route *router.Route) *kinesis.Kinesis {
    var auth *kinesis.AuthCredentials
    var err error

    if getEnvVar("AWS_ACCESS_KEY", "") != "" {
        // Get auth from env variables AWS_ACCESS_KEY and AWS_SECRET_KEY
        auth, err = kinesis.NewAuthFromEnv()
        if err != nil {
            fmt.Printf("Unable to retrieve authentication credentials from the environment: %v", err)
            os.Exit(1)
        }
    } else {
        // Get auth from meta-data-server
        auth, err = kinesis.NewAuthFromMetadata()
        if err != nil {
            fmt.Printf("Unable to retrieve authentication credentials from the the meta-data-server: %v", err)
            os.Exit(1)
        }
    }

    // AWS region
    aws_region := kinesis.NewRegionFromEnv()
    fmt.Printf("# KINESIS Adapter - Using region: %s\n", aws_region)

    return kinesis.New(auth, aws_region)
}

func getKinesisConfig(route_helper RouteHelper) batchproducer.Config {
    return batchproducer.Config{
        AddBlocksWhenBufferFull: route_helper.route.Options["add_blocks_when_buffer_full"] == "true",
        BufferSize:              route_helper.getOptInt("buffer_size", 10000),
        FlushInterval:           time.Duration(route_helper.getOptInt("flush_interval", 1)) * time.Second,
        BatchSize:               route_helper.getOptInt("batch_size", 10),
        MaxAttemptsPerRecord:    route_helper.getOptInt("max_attempts_per_record", 10),
        StatInterval:            time.Duration(route_helper.getOptInt("stat_interval", 1)) * time.Second,
        Logger:                  log.New(os.Stderr, "kinesis: ", log.LstdFlags),
    }
}

func getEnvVar(name, dfault string) string {
    value := os.Getenv(name)
    if value == "" {
        value = dfault
    }
    return value
}

func (rh *RouteHelper) getOptInt(key string, dfault int) int {
    value := dfault
    value_string, ok := rh.route.Options[key];
    if ok && value_string != "" {
        if i, err := strconv.ParseInt(value_string, 10, 0); err == nil {
            value = int(i)
        } else {
            value = dfault
        }
    }
    return value
}

func (ka *KinesisAdapter) Stream(logstream chan *router.Message) {
	// Start the producer
	err := ka.batch_producer.Start()
	if err != nil {
		fmt.Printf("Unable to start batchproducer: %v", err)
		os.Exit(1)
	}

	// Register stop
	defer ka.batch_producer.Stop()

	// Handle log messages
	mute := false
	for m := range logstream {
		msg := createLogstashMessage(m, ka.docker_host, ka.use_v0)

		// Create json from log message
        log_json, err := json.Marshal(msg)
        if err != nil {
        	if !mute {
                log.Println("logspoutkinesis: error on json.Marshal (muting until restored): %v\n", err)
                mute = true
            }
            continue
        }

        // Send json to kinesis
        err = ka.batch_producer.Add(log_json, ka.partition_key)
        if err != nil {
        	if !mute {
                log.Println("logspoutkinesis: error on batchproducer.Stop (muting until restored): %v\n", err)
                mute = true
            }
            continue
        }
 		
		// Unmute
        mute = false
	}
}	

func splitImage(image string) (string, string, string) {
    var tag string
    var repo string

    parts := strings.Split(image, "/")
    parts_len := len(parts)
    if parts_len > 2 {
        repo = parts[0]
        image = strings.Join(parts[1:parts_len], "/")
    }

    parts = strings.SplitN(image, ":", 2)

    if len(parts) == 2 {
        image = parts[0]
        tag = parts[1]
    }

    return image, tag, repo
}

func createLogstashMessage(m *router.Message, docker_host string, use_v0 bool) interface{} {
    image_name, image_tag, image_repo := splitImage(m.Container.Config.Image)
    cid := m.Container.ID[0:12]
    name := m.Container.Name[1:]
    labels := m.Container.Config.Labels
    timestamp := m.Time.Format(time.RFC3339Nano)

    if use_v0 {
        return LogstashMessageV0{
            Message:    m.Data,
            Timestamp:  timestamp,
            Sourcehost: m.Container.Config.Hostname,
            Fields:     LogstashFields{
                Docker: DockerFields{
                    CID:        cid,
                    Name:       name,
                    Image:      image_name,
                    ImageTag:   image_tag,
                    ImageRepo:  image_repo,
                    Source:     m.Source,
                    DockerHost: docker_host,
                    Labels: labels,
                },
            },
        }
    }

    return LogstashMessageV1{
        Message:    m.Data,
        Timestamp:  timestamp,
        Sourcehost: m.Container.Config.Hostname,
        Fields:     DockerFields{
            CID:        cid,
            Name:       name,
            Image:      image_name,
            ImageTag:   image_tag,
            Source:     m.Source,
            DockerHost: docker_host,
            Labels: labels,
        },
    }
}
